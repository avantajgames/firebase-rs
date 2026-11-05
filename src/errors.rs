use std::{
    error::Error,
    fmt::{Display, Formatter},
};

pub type UrlParseResult<T> = Result<T, UrlParseError>;

#[derive(Debug)]
pub enum UrlParseError {
    NoPath,
    NotHttps,
    Parser(url::ParseError),
}

impl Error for UrlParseError {}

impl Display for UrlParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UrlParseError::NoPath => write!(f, "URL path is missing."),
            UrlParseError::NotHttps => write!(f, "The URL protocol should be https."),
            UrlParseError::Parser(e) => write!(f, "Error while parsing the URL: {}", e),
        }
    }
}

pub type RequestResult<T> = Result<T, RequestError>;

#[derive(Debug)]
pub enum RequestError {
    NotJSON,
    NoUTF8,
    /// The request never got an HTTP response (connect, timeout, TLS, ...).
    NetworkError(String),
    /// The database answered with a status other than 200 or 412.
    HttpStatus(u16),
    SerializeError,
    LimitExceeded,
}

impl Error for RequestError {}

impl RequestError {
    /// Worth one more try: no response at all, rate limiting, or a server-side failure.
    pub fn is_transient(&self) -> bool {
        match self {
            RequestError::NetworkError(_) => true,
            RequestError::HttpStatus(status) => *status == 429 || *status >= 500,
            _ => false,
        }
    }
}

impl Display for RequestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestError::NotJSON => write!(f, "Invalid JSON"),
            RequestError::NoUTF8 => write!(f, "Utf8 error"),
            RequestError::NetworkError(cause) => write!(f, "Network error: {}", cause),
            RequestError::HttpStatus(status) => write!(f, "Unexpected HTTP status {}", status),
            RequestError::SerializeError => write!(f, "Serialize error"),
            RequestError::LimitExceeded => write!(f, "Limit exceeded"),
        }
    }
}

#[derive(Debug)]
pub enum ServerEventError {
    ConnectionError,
}

impl Error for ServerEventError {}

impl Display for ServerEventError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerEventError::ConnectionError => write!(f, "Connection error for server events"),
        }
    }
}
