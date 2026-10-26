use crate::errors::UrlParseResult;
use crate::UrlParseError;
use url::{Host, Url};

pub fn check_uri(uri: &str) -> UrlParseResult<Url> {
    let uri = Url::parse(uri);

    let uri = match uri {
        Ok(res) => res,
        Err(err) => return Err(UrlParseError::Parser(err)),
    };

    // Plain HTTP is allowed only for a loopback host, e.g. the Firebase Emulator.
    let local_http = uri.scheme() == "http"
        && match uri.host() {
            Some(Host::Domain(domain)) => domain == "localhost",
            Some(Host::Ipv4(ip)) => ip.is_loopback(),
            Some(Host::Ipv6(ip)) => ip.is_loopback(),
            None => false,
        };

    if uri.scheme() != "https" && !local_http {
        return Err(UrlParseError::NotHttps);
    }

    Ok(uri)
}

#[cfg(test)]
mod tests {
    use super::check_uri;

    #[test]
    fn allows_http_only_for_loopback() {
        assert!(check_uri("https://example.firebaseio.com").is_ok());
        assert!(check_uri("http://localhost:9000/?ns=x").is_ok());
        assert!(check_uri("http://127.0.0.1:9000/?ns=x").is_ok());
        assert!(check_uri("http://[::1]:9000/?ns=x").is_ok());
        assert!(check_uri("http://example.firebaseio.com").is_err());
        assert!(check_uri("http://10.0.0.1:9000").is_err());
    }
}
