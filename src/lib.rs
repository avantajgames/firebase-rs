use constants::{Method, Response, AUTH};
use errors::{RequestResult, UrlParseResult};
use params::Params;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};
use std::fmt::Debug;
use std::sync::OnceLock;
use url::Url;
use utils::check_uri;

use crate::sse::ServerEvents;

pub use errors::{RequestError, ServerEventError, UrlParseError};

mod constants;
mod errors;
mod params;
mod sse;
mod utils;

/// One client for all requests so connections to the database are reused.
fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

#[derive(Debug)]
pub struct Firebase {
    uri: Url,
}

impl Firebase {
    /// ```rust
    /// use firebase_rs::Firebase;
    ///
    /// let firebase = Firebase::new("https://myfirebase.firebaseio.com").unwrap();
    /// ```
    pub fn new(uri: &str) -> UrlParseResult<Self>
    where
        Self: Sized,
    {
        match check_uri(&uri) {
            Ok(uri) => Ok(Self { uri }),
            Err(err) => Err(err),
        }
    }

    /// ```rust
    /// const URI: &str = "...";
    /// const AUTH_KEY: &str = "...";
    ///
    /// use firebase_rs::Firebase;
    ///
    /// let firebase = Firebase::auth("https://myfirebase.firebaseio.com", AUTH_KEY).unwrap();
    /// ```
    pub fn auth(uri: &str, auth_key: &str) -> UrlParseResult<Self>
    where
        Self: Sized,
    {
        match check_uri(&uri) {
            Ok(mut uri) => {
                uri.set_query(Some(&format!("{}={}", AUTH, auth_key)));
                Ok(Self { uri })
            }
            Err(err) => Err(err),
        }
    }

    /// ```rust
    /// use firebase_rs::Firebase;
    ///
    /// # async fn run() {
    /// let firebase = Firebase::new("https://myfirebase.firebaseio.com").unwrap().with_params().start_at(1).order_by("name").equal_to(5).finish();
    /// let result = firebase.get::<String>().await;
    /// # }
    /// ```
    pub fn with_params(&self) -> Params {
        let uri = self.uri.clone();
        Params::new(uri)
    }

    /// To use simple interface with synchronous callbacks, pair with `.listen()`:
    /// ```rust
    /// use firebase_rs::Firebase;
    /// # async fn run() {
    /// let firebase = Firebase::new("https://myfirebase.firebaseio.com").unwrap().at("users");
    /// let stream = firebase.with_realtime_events().unwrap();
    /// stream.listen(|event_type, data| {
    ///                     println!("{:?} {:?}" ,event_type, data);
    ///                 }, |err| println!("{:?}" ,err), false).await;
    /// # }
    /// ```
    ///
    /// To use streaming interface for async code, pair with `.stream()`:
    /// ```rust
    /// use firebase_rs::Firebase;
    /// use futures_util::StreamExt;
    ///
    /// # async fn run() {
    /// let firebase = Firebase::new("https://myfirebase.firebaseio.com").unwrap().at("users");
    /// let stream = firebase.with_realtime_events()
    ///              .unwrap()
    ///              .stream(true);
    /// stream.for_each(|event| {
    ///           match event {
    ///               Ok((event_type, maybe_data)) => println!("{:?} {:?}" ,event_type, maybe_data),
    ///               Err(err) => println!("{:?}" ,err),
    ///           }
    ///           futures_util::future::ready(())
    ///        }).await;
    /// # }
    /// ```
    pub fn with_realtime_events(&self) -> Option<ServerEvents> {
        ServerEvents::new(self.uri.as_str())
    }

    /// ```rust
    /// use firebase_rs::Firebase;
    ///
    /// let firebase = Firebase::new("https://myfirebase.firebaseio.com").unwrap().at("users").at("USER_ID").at("f69111a8a5258c15286d3d0bd4688c55");
    /// ```
    pub fn at(&self, path: &str) -> Self {
        let uri = self.build_uri(path);

        Firebase { uri }
    }

    fn build_uri(&self, path: &str) -> Url {
        let mut new_path = String::new();

        if let Some(segments) = self.uri.path_segments() {
            for segment in segments {
                let clean_segment = segment.trim_end_matches(".json");
                new_path.push_str(clean_segment);
                new_path.push('/');
            }
        }

        new_path.push_str(path);

        let final_path = if new_path.ends_with(".json") {
            new_path.trim_end_matches(".json").to_string()
        } else {
            new_path
        };

        let mut uri = self.uri.clone();
        uri.set_path(&format!("{}.json", final_path));

        uri
    }

    /// ```rust
    /// use firebase_rs::Firebase;
    ///
    /// let firebase = Firebase::new("https://myfirebase.firebaseio.com").unwrap().at("users");
    /// let uri = firebase.get_uri();
    /// ```
    pub fn get_uri(&self) -> String {
        self.uri.to_string()
    }

    /// Sends the request under the retry policy its method and body allow (see [`RetryPolicy`]).
    async fn request(&self, method: Method, data: Option<Value>, include_etag: bool, etag: Option<&str>) -> RequestResult<Response> {
        let policy = match method {
            Method::GET => &READS,
            Method::PUT if is_repeatable(data.as_ref()) => &PUTS,
            _ => &NO_RETRY,
        };
        self.request_with(policy, &method, data.as_ref(), include_etag, etag).await
    }

    async fn request_with(&self, policy: &RetryPolicy, method: &Method, data: Option<&Value>, include_etag: bool, etag: Option<&str>) -> RequestResult<Response> {
        let mut result = self.request_once(method, data, include_etag, etag).await;
        let mut retried = false;
        for &delay_ms in policy.delays_ms {
            if !matches!(&result, Err(err) if (policy.retry_on)(err)) {
                break;
            }
            backoff(delay_ms).await;
            result = self.request_once(method, data, include_etag, etag).await;
            retried = true;
        }
        let attempt = result?;
        if retried && attempt.precondition_failed && stored_value_is(&attempt.response.data, data) {
            // A reset hid the 200 of an earlier attempt: the stored value is ours. Answer as
            // that 200 would have, so a caller does not take its own write for a conflict.
            let etag = if include_etag { attempt.response.etag } else { None };
            return Ok(Response { etag, data: attempt.response.data });
        }
        Ok(attempt.response)
    }

    async fn request_once(&self, method: &Method, data: Option<&Value>, include_etag: bool, etag: Option<&str>) -> RequestResult<Attempt> {
        let client = http_client();
        let mut request_builder = match method {
            Method::GET => client.get(self.uri.to_string()),
            Method::PUT | Method::PATCH | Method::POST => {
                if data.is_none() {
                    return Err(RequestError::SerializeError);
                }
                let builder = if *method == Method::PUT {
                    client.put(self.uri.to_string())
                } else if *method == Method::POST {
                    client.post(self.uri.to_string())
                } else {
                    client.patch(self.uri.to_string())
                };
                builder.json(&data)
            }
            Method::DELETE => client.delete(self.uri.to_string()),
        };

        if include_etag {
            request_builder = request_builder.header("X-Firebase-ETag", "true");
        }

        if let Some(etag_value) = etag {
            request_builder = request_builder.header("if-match", etag_value);
        }

        let request = request_builder.send().await;

        match request {
            Ok(response) => {
                let etag = response.headers().get("ETag").map(|v| v.to_str().unwrap().to_string());
                match response.status() {
                    StatusCode::OK => {
                        let mut response_text = response.text().await.unwrap_or_default();
                        if response_text == "null" {
                            response_text = String::default();
                        }
                        Ok(Attempt { response: Response { etag, data: response_text }, precondition_failed: false })
                    }
                    StatusCode::PRECONDITION_FAILED => {
                        // create a new response with the etag value and with a new response body
                        let response_text = response.text().await.unwrap_or_default();
                        Ok(Attempt { response: Response { etag, data: response_text }, precondition_failed: true })
                    }
                    status => Err(RequestError::HttpStatus(status.as_u16())),
                }
            }
            Err(err) => Err(RequestError::NetworkError(describe_transport_error(err))),
        }
    }

    pub async fn update_atomically_by(&self, value: i64, limit_min: Option<i64>, limit_max: Option<i64>, etag: Option<String>) -> RequestResult<Response> {
        Box::pin(async move {
            if let Some(etag) = etag {
                let data = json!(value);
                let updated_response = self.request_with(&NO_RETRY, &Method::PUT, Some(&data), false, Some(etag.as_str())).await;
                if let Ok(response) = &updated_response {
                    if response.etag.is_none() {
                        return updated_response;
                    }
                    let next_value = match Self::check_current_before_update(value, limit_min, limit_max, response.data.as_str()) {
                        Ok(value) => value,
                        Err(value) => return value,
                    };
                    return self.update_atomically_by(next_value, limit_min, limit_max, response.etag.clone()).await;
                }
                return updated_response;
            }
            let response = self.request(Method::GET, None, true, None).await;
            match response {
                Ok(response) => {
                    let new_value = match Self::check_current_before_update(value, limit_min, limit_max, response.data.as_str()) {
                        Ok(value) => value,
                        Err(value) => return value,
                    };
                    self.update_atomically_by(new_value, limit_min, limit_max, response.etag).await
                }
                Err(err) => Err(err),
            }
        }).await
    }

    fn check_current_before_update(value: i64, limit_min: Option<i64>, limit_max: Option<i64>, data: &str) -> Result<i64, RequestResult<Response>> {
        let new_value: i64 = serde_json::from_str(data).unwrap();
        if let Some(limit) = limit_max {
            if value > 0 && new_value >= limit {
                return Err(Err(RequestError::LimitExceeded));
            }
        }
        if let Some(limit) = limit_min {
            if new_value == limit {
                return Err(Err(RequestError::LimitExceeded));
            }
        }
        let next_value = new_value + value;
        Ok(next_value)
    }

    async fn request_generic<T>(&self, method: Method) -> RequestResult<Option<T>>
    where
        T: Serialize + DeserializeOwned + Debug,
    {
        let request = self.request(method, None, false, None).await;

        match request {
            Ok(response) => {
                if response.data.is_empty() {
                    return Ok(None);
                }
                let data: T = serde_json::from_str(response.data.as_str()).unwrap();
                Ok(Some(data))
            }
            Err(err) => Err(err),
        }
    }

    /// ```rust
    /// use firebase_rs::Firebase;
    /// use serde::{Serialize, Deserialize};
    ///
    /// #[derive(Serialize, Deserialize, Debug)]
    /// struct User {
    ///    name: String
    /// }
    ///
    /// # async fn run() {
    /// let user = User { name: String::default() };
    /// let firebase = Firebase::new("https://myfirebase.firebaseio.com").unwrap().at("users");
    /// let users = firebase.set(&user).await;
    /// # }
    /// ```
    pub async fn set<T>(&self, data: &T) -> RequestResult<Response>
    where
        T: Serialize + DeserializeOwned + Debug,
    {
        let data = serde_json::to_value(&data).unwrap();
        self.request(Method::POST, Some(data), false, None).await
    }

    pub async fn post_json(&self, data: serde_json::Value) -> RequestResult<Response>
    {
        self.request(Method::POST, Some(data), false, None).await
    }

    pub async fn put_json(&self, data: serde_json::Value) -> RequestResult<Response>
    {
        self.request(Method::PUT, Some(data), false, None).await
    }

    /// ```rust
    /// use firebase_rs::Firebase;
    /// use serde::{Serialize, Deserialize};
    ///
    /// #[derive(Serialize, Deserialize, Debug)]
    /// struct User {
    ///    name: String
    /// }
    ///
    /// # async fn run() {
    /// let user = User { name: String::default() };
    /// let mut firebase = Firebase::new("https://myfirebase.firebaseio.com").unwrap().at("users");
    /// let users = firebase.set_with_key("myKey", &user, None, None).await;
    /// # }
    /// ```
    pub async fn set_with_key<T>(&mut self, key: &str, data: &T, include_etag: Option<bool>, etag: Option<&str>) -> RequestResult<Response>
    where
        T: Serialize + DeserializeOwned + Debug,
    {
        self.uri = self.build_uri(key);
        let data = serde_json::to_value(&data).unwrap();

        self.request(Method::PUT, Some(data), include_etag.unwrap_or(false), etag).await
    }

    /// ```rust
    /// use std::collections::HashMap;
    /// use firebase_rs::Firebase;
    /// use serde::{Serialize, Deserialize};
    ///
    /// #[derive(Serialize, Deserialize, Debug)]
    /// struct User {
    ///    name: String
    /// }
    ///
    /// # async fn run() {
    /// let firebase = Firebase::new("https://myfirebase.firebaseio.com").unwrap().at("users");
    /// let users = firebase.get::<HashMap<String, User>>().await;
    /// # }
    /// ```
    pub async fn get_as_string(&self, include_etag: Option<bool>, etag: Option<&str>) -> RequestResult<Response> {
        self.request(Method::GET, None, include_etag.unwrap_or(false), etag).await
    }

    /// ```rust
    /// use std::collections::HashMap;
    /// use firebase_rs::Firebase;
    /// use serde::{Serialize, Deserialize};
    ///
    /// #[derive(Serialize, Deserialize, Debug)]
    /// struct User {
    ///     name: String
    /// }
    ///
    /// # async fn run() {
    /// let firebase = Firebase::new("https://myfirebase.firebaseio.com").unwrap().at("users").at("USER_ID");
    /// let user = firebase.get::<User>().await;
    ///
    ///  // OR
    ///
    /// let firebase = Firebase::new("https://myfirebase.firebaseio.com").unwrap().at("users");
    /// let user = firebase.get::<HashMap<String, User>>().await;
    /// # }
    /// ```
    pub async fn get<T>(&self) -> RequestResult<Option<T>>
    where
        T: Serialize + DeserializeOwned + Debug,
    {
        self.request_generic::<T>(Method::GET).await
    }

    /// ```rust
    /// use firebase_rs::Firebase;
    ///
    /// # async fn run() {
    /// let firebase = Firebase::new("https://myfirebase.firebaseio.com").unwrap().at("users").at("USER_ID");
    /// firebase.delete().await;
    /// # }
    /// ```
    pub async fn delete(&self) -> RequestResult<Response> {
        self.request(Method::DELETE, None, false, None).await
    }

    /// ```rust
    /// use firebase_rs::Firebase;
    /// use serde::{Serialize, Deserialize};
    ///
    /// #[derive(Serialize, Deserialize, Debug)]
    /// struct User {
    ///     name: String
    /// }
    ///
    /// # async fn run() {
    /// let user = User { name: String::default() };
    /// let firebase = Firebase::new("https://myfirebase.firebaseio.com").unwrap().at("users").at("USER_ID");
    /// let users = firebase.update(&user, None, None).await;
    /// # }
    /// ```
    pub async fn update<T>(&self, data: &T, include_etag: Option<bool>, etag: Option<&str>) -> RequestResult<Response>
    where
        T: DeserializeOwned + Serialize + Debug,
    {
        let value = serde_json::to_value(&data).unwrap();
        self.request(Method::PATCH, Some(value), include_etag.unwrap_or(false), etag).await
    }
}

/// Which failures of a request are tried again, and after which pauses.
struct RetryPolicy {
    /// One pause per retry, before it; [`RETRY_JITTER_MS`] at most is added to each.
    delays_ms: &'static [u64],
    retry_on: fn(&RequestError) -> bool,
}

/// A GET changes nothing: retried after no response, 429 or 5xx.
const READS: RetryPolicy = RetryPolicy { delays_ms: &[200, 400], retry_on: RequestError::is_transient };
/// A PUT without server values writes the same thing again, so it is retried once, and only when
/// there was no response at all. A conditional one that did land is settled in `request_with`.
const PUTS: RetryPolicy = RetryPolicy { delays_ms: &[200], retry_on: |err| matches!(err, RequestError::NetworkError(_)) };
/// POST (push), PATCH (may carry increments), DELETE, PUTs with `.sv`, and the PUT inside
/// `update_atomically_by` (a step of an increment).
const NO_RETRY: RetryPolicy = RetryPolicy { delays_ms: &[], retry_on: |_| false };
/// So that requests that failed together do not retry in step.
const RETRY_JITTER_MS: u64 = 100;

/// One HTTP exchange that got a response the caller can use (200 or 412).
struct Attempt {
    response: Response,
    precondition_failed: bool,
}

async fn backoff(delay_ms: u64) {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    tokio::time::sleep(std::time::Duration::from_millis(delay_ms + nanos % (RETRY_JITTER_MS + 1))).await;
}

/// A PUT body can be sent twice unless it asks the server to compute a value (`.sv`).
fn is_repeatable(data: Option<&Value>) -> bool {
    fn has_server_value(value: &Value) -> bool {
        match value {
            Value::Object(fields) => fields.contains_key(".sv") || fields.values().any(has_server_value),
            Value::Array(items) => items.iter().any(has_server_value),
            _ => false,
        }
    }
    data.map_or(false, |value| !has_server_value(value))
}

/// Whether the value a 412 reports as stored is the one this request sent.
fn stored_value_is(stored: &str, sent: Option<&Value>) -> bool {
    let stored = if stored.is_empty() { Some(Value::Null) } else { serde_json::from_str::<Value>(stored).ok() };
    matches!((stored, sent), (Some(stored), Some(sent)) if stored == *sent)
}

/// A transport error as text: its whole cause chain (reqwest's own message only says
/// "error sending request"), without the request URL, which carries the access token.
fn describe_transport_error(err: reqwest::Error) -> String {
    let err = err.without_url();
    let mut text = err.to_string();
    let mut source = std::error::Error::source(&err);
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

#[cfg(test)]
mod tests {
    use crate::{Firebase, RequestError, UrlParseError};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use serde_json::{json, Value};
    use std::time::{Duration, Instant};

    /// What the scripted server does with one request.
    #[derive(Clone, Copy)]
    enum Reply {
        /// Answer with this status and body (and an ETag header).
        With(u16, &'static str),
        /// Read the request and close the connection without answering, as a reset does.
        Drop,
    }

    /// A loopback HTTP server that handles each request with the next entry of `script` (the
    /// last one repeats). Returns its URL and the number of requests it has received.
    async fn scripted_server(script: &'static [Reply]) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let served = Arc::new(AtomicUsize::new(0));
        let counter = served.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let n = counter.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                match script[n.min(script.len() - 1)] {
                    Reply::Drop => drop(socket),
                    Reply::With(status, body) => {
                        let reply = format!("HTTP/1.1 {status} X\r\netag: tag{n}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
                        let _ = socket.write_all(reply.as_bytes()).await;
                    }
                }
            }
        });
        (url, served)
    }

    const OK: Reply = Reply::With(200, "{\"a\":1}");

    fn db(url: &str) -> Firebase {
        Firebase::new(url).unwrap().at("x")
    }

    #[tokio::test]
    async fn a_read_is_retried_after_a_server_error() {
        let (url, served) = scripted_server(&[Reply::With(503, ""), OK]).await;
        let response = db(&url).get_as_string(None, None).await.unwrap();
        assert_eq!(response.data, "{\"a\":1}");
        assert_eq!(served.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_read_is_retried_after_a_reset() {
        let (url, served) = scripted_server(&[Reply::Drop, Reply::Drop, OK]).await;
        let response = db(&url).get_as_string(None, None).await.unwrap();
        assert_eq!(response.data, "{\"a\":1}");
        assert_eq!(served.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn a_read_backs_off_and_gives_up_after_two_retries_naming_the_status() {
        let (url, served) = scripted_server(&[Reply::With(500, "")]).await;
        let started = Instant::now();
        let err = db(&url).get_as_string(None, None).await.unwrap_err();
        assert!(started.elapsed() >= Duration::from_millis(600), "{:?}", started.elapsed());
        assert!(matches!(err, RequestError::HttpStatus(500)));
        assert_eq!(err.to_string(), "Unexpected HTTP status 500");
        assert_eq!(served.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn a_client_error_is_not_retried() {
        let (url, served) = scripted_server(&[Reply::With(401, "")]).await;
        let err = db(&url).get_as_string(None, None).await.unwrap_err();
        assert!(matches!(err, RequestError::HttpStatus(401)));
        assert_eq!(served.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_put_that_got_a_response_is_not_retried() {
        let (url, served) = scripted_server(&[Reply::With(503, ""), OK]).await;
        let err = db(&url).put_json(json!(1)).await.unwrap_err();
        assert!(matches!(err, RequestError::HttpStatus(503)));
        assert_eq!(served.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_put_is_retried_once_after_a_reset() {
        let (url, served) = scripted_server(&[Reply::Drop, OK]).await;
        let response = db(&url).put_json(json!({"a": 1})).await.unwrap();
        assert_eq!(response.data, "{\"a\":1}");
        assert_eq!(served.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_put_gives_up_after_one_retry() {
        let (url, served) = scripted_server(&[Reply::Drop]).await;
        let err = db(&url).put_json(json!(1)).await.unwrap_err();
        assert!(matches!(err, RequestError::NetworkError(_)), "{err}");
        assert_eq!(served.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_conditional_put_whose_first_attempt_landed_is_a_success() {
        // The reset hid the 200; the retry finds our own value under a new ETag.
        let (url, served) = scripted_server(&[Reply::Drop, Reply::With(412, "{\"a\":1}")]).await;
        let response = db(&url).set_with_key("k", &json!({"a": 1}), None, Some("old")).await.unwrap();
        assert_eq!(response.etag, None, "a success has no ETag unless one was asked for");
        assert_eq!(response.data, "{\"a\":1}");
        assert_eq!(served.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_conditional_put_that_landed_keeps_the_etag_when_asked_for() {
        let (url, _) = scripted_server(&[Reply::Drop, Reply::With(412, "null")]).await;
        let response = db(&url).set_with_key("k", &Value::Null, Some(true), Some("old")).await.unwrap();
        assert_eq!(response.etag.as_deref(), Some("tag1"));
    }

    #[tokio::test]
    async fn a_conditional_put_retry_still_reports_a_real_conflict() {
        let (url, served) = scripted_server(&[Reply::Drop, Reply::With(412, "{\"a\":2}")]).await;
        let response = db(&url).set_with_key("k", &json!({"a": 1}), None, Some("old")).await.unwrap();
        assert_eq!(response.etag.as_deref(), Some("tag1"));
        assert_eq!(response.data, "{\"a\":2}");
        assert_eq!(served.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_put_with_a_server_value_is_not_retried() {
        let (url, served) = scripted_server(&[Reply::Drop, OK]).await;
        let increment = json!({"score": {".sv": {"increment": 5}}});
        assert!(db(&url).put_json(increment).await.is_err());
        assert_eq!(served.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_push_is_not_retried() {
        let (url, served) = scripted_server(&[Reply::Drop]).await;
        assert!(db(&url).post_json(json!(1)).await.is_err());
        assert!(db(&url).set(&json!(1)).await.is_err());
        assert_eq!(served.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_patch_or_delete_is_not_retried() {
        let (url, served) = scripted_server(&[Reply::Drop, OK]).await;
        assert!(db(&url).update(&json!({"a": 1}), None, None).await.is_err());
        assert_eq!(served.load(Ordering::SeqCst), 1);
        let (url, served) = scripted_server(&[Reply::Drop, OK]).await;
        assert!(db(&url).delete().await.is_err());
        assert_eq!(served.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_atomic_increment_does_not_retry_its_write() {
        let (url, served) = scripted_server(&[Reply::With(200, "5"), Reply::Drop, Reply::With(200, "6")]).await;
        let err = db(&url).update_atomically_by(1, None, None, None).await.unwrap_err();
        assert!(matches!(err, RequestError::NetworkError(_)), "{err}");
        assert_eq!(served.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn no_response_is_a_network_error_with_its_cause() {
        // Bind and drop: nothing listens on the port any more.
        let port = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port();
        let err = Firebase::new(&format!("http://127.0.0.1:{port}/")).unwrap().get_as_string(None, None).await.unwrap_err();
        assert!(matches!(&err, RequestError::NetworkError(cause) if !cause.is_empty()), "{err}");
    }

    #[tokio::test]
    async fn a_network_error_names_its_cause_but_not_the_url_or_token() {
        let port = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}/?access_token=secret_token");
        let err = Firebase::new(&url).unwrap().at("members").at("uid").get_as_string(None, None).await.unwrap_err();
        let text = err.to_string();
        assert!(!text.contains("secret_token") && !text.contains("access_token") && !text.contains("members"), "{text}");
        assert!(text.to_lowercase().contains("connect"), "the cause should be named: {text}");
    }

    const URI: &str = "https://firebase_id.firebaseio.com";
    const URI_WITH_SLASH: &str = "https://firebase_id.firebaseio.com/";
    const URI_NON_HTTPS: &str = "http://firebase_id.firebaseio.com/";

    #[tokio::test]
    async fn simple() {
        let firebase = Firebase::new(URI).unwrap();
        assert_eq!(URI_WITH_SLASH.to_string(), firebase.get_uri());
    }

    #[tokio::test]
    async fn non_https() {
        let firebase = Firebase::new(URI_NON_HTTPS).map_err(|e| e.to_string());
        assert_eq!(
            firebase.err(),
            Some(String::from(UrlParseError::NotHttps.to_string()))
        );
    }

    #[tokio::test]
    async fn with_auth() {
        let firebase = Firebase::auth(URI, "auth_key").unwrap();
        assert_eq!(
            format!("{}/?auth=auth_key", URI.to_string()),
            firebase.get_uri()
        );
    }

    #[tokio::test]
    async fn with_sse_events() {
        // TODO: SSE Events Test
    }
}
