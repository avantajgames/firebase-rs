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

    /// Reads are retried once on a transient failure: a GET changes nothing, so repeating it
    /// is safe. Writes are not, since a PATCH with a server-side increment or a POST would apply
    /// twice.
    async fn request(&self, method: Method, data: Option<Value>, include_etag: bool, etag: Option<&str>) -> RequestResult<Response> {
        let attempts = if method == Method::GET { 2 } else { 1 };
        let mut result = self.request_once(&method, data.as_ref(), include_etag, etag).await;
        for _ in 1..attempts {
            match &result {
                Err(err) if err.is_transient() => {
                    result = self.request_once(&method, data.as_ref(), include_etag, etag).await;
                }
                _ => break,
            }
        }
        result
    }

    async fn request_once(&self, method: &Method, data: Option<&Value>, include_etag: bool, etag: Option<&str>) -> RequestResult<Response> {
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
                        Ok(Response { etag, data: response_text })
                    }
                    StatusCode::PRECONDITION_FAILED => {
                        // create a new response with the etag value and with a new response body
                        let response_text = response.text().await.unwrap_or_default();
                        Ok(Response {etag, data: response_text })
                    }
                    status => Err(RequestError::HttpStatus(status.as_u16())),
                }
            }
            Err(err) => Err(RequestError::NetworkError(err.to_string())),
        }
    }

    pub async fn update_atomically_by(&self, value: i64, limit_min: Option<i64>, limit_max: Option<i64>, etag: Option<String>) -> RequestResult<Response> {
        Box::pin(async move {
            if let Some(etag) = etag {
                let data = json!(value);
                let updated_response = self.request(Method::PUT, Some(data), false, Some(etag).as_deref()).await;
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

#[cfg(test)]
mod tests {
    use crate::{Firebase, RequestError, UrlParseError};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// A loopback HTTP server that answers each request with the next status in `statuses`
    /// (the last one repeats). Returns its URL and the number of requests it has received.
    async fn scripted_server(statuses: &'static [u16]) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let served = Arc::new(AtomicUsize::new(0));
        let counter = served.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let n = counter.fetch_add(1, Ordering::SeqCst);
                let status = statuses[n.min(statuses.len() - 1)];
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                let body = "{\"a\":1}";
                let reply = format!("HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
                let _ = socket.write_all(reply.as_bytes()).await;
            }
        });
        (url, served)
    }

    #[tokio::test]
    async fn a_read_is_retried_once_after_a_server_error() {
        let (url, served) = scripted_server(&[503, 200]).await;
        let response = Firebase::new(&url).unwrap().at("x").get_as_string(None, None).await.unwrap();
        assert_eq!(response.data, "{\"a\":1}");
        assert_eq!(served.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_read_gives_up_after_the_second_failure_and_names_the_status() {
        let (url, served) = scripted_server(&[500]).await;
        let err = Firebase::new(&url).unwrap().at("x").get_as_string(None, None).await.unwrap_err();
        assert!(matches!(err, RequestError::HttpStatus(500)));
        assert_eq!(err.to_string(), "Unexpected HTTP status 500");
        assert_eq!(served.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_client_error_is_not_retried() {
        let (url, served) = scripted_server(&[401]).await;
        let err = Firebase::new(&url).unwrap().at("x").get_as_string(None, None).await.unwrap_err();
        assert!(matches!(err, RequestError::HttpStatus(401)));
        assert_eq!(served.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_write_is_never_retried() {
        let (url, served) = scripted_server(&[503, 200]).await;
        let err = Firebase::new(&url).unwrap().at("x").put_json(serde_json::json!(1)).await.unwrap_err();
        assert!(matches!(err, RequestError::HttpStatus(503)));
        assert_eq!(served.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn no_response_is_a_network_error_with_its_cause() {
        // Bind and drop: nothing listens on the port any more.
        let port = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port();
        let err = Firebase::new(&format!("http://127.0.0.1:{port}/")).unwrap().get_as_string(None, None).await.unwrap_err();
        assert!(matches!(&err, RequestError::NetworkError(cause) if !cause.is_empty()), "{err}");
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
