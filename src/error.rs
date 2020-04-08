use failure::Fail;
use rusoto_core::{request::BufferedHttpResponse, RusotoError};
use serde::Deserialize;
use std::error::Error as StdError;
use tracing::trace;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct ErrorResponse {
    error: ErrorResponseError,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct ErrorResponseError {
    code: String,
    message: String,
}

#[derive(Debug, Fail)]
pub enum NError {
    #[fail(display = "failed with internal service request: {} - {}", type_, msg)]
    Rusoto { type_: String, msg: String },
    #[fail(display = "failed with http dispatcher: {}", _0)]
    HttpDispatch(String),
    #[fail(display = "too many requests: {}", _0)]
    Throttling(String),
    #[fail(display = "validation failure: {}", _0)]
    Validation(String),
    #[fail(display = "dag failure: {}", _0)]
    Dag(String),
    #[fail(display = "failed with provided credentials: {}", e)]
    InvalidCredentials {
        e: rusoto_credential::CredentialsError,
    },
    #[fail(display = "failed parsing the region: {}", e)]
    InvalidRegion {
        e: rusoto_core::region::ParseRegionError,
    },
    #[fail(display = "TLS provider failure: {}", e)]
    HttpsConnector { e: rusoto_core::request::TlsError },
}

impl<E: StdError + 'static> From<RusotoError<E>> for NError {
    fn from(err: RusotoError<E>) -> Self {
        match &err {
            RusotoError::Unknown(BufferedHttpResponse { ref body, .. }) => {
                if let Ok(ErrorResponse { error }) =
                    serde_xml_rs::from_reader::<_, ErrorResponse>(body.as_ref())
                {
                    match error.code.as_str() {
                        "Throttling" => return NError::Throttling(error.message),
                        "ValidationError" => return NError::Validation(error.message),
                        code => trace!("unmatched error code {}", code),
                    }
                }
                NError::Rusoto {
                    type_: "Unknown".to_string(),
                    msg: format!("{}", err),
                }
            }
            RusotoError::Service(e) => NError::Rusoto {
                type_: format!("{:?}", e),
                msg: format!("{}", err),
            },
            RusotoError::HttpDispatch(e) => NError::HttpDispatch(e.to_string()),
            _ => NError::Rusoto {
                type_: "Unknown".to_string(),
                msg: format!("{}", err),
            },
        }
    }
}

impl From<rusoto_core::region::ParseRegionError> for NError {
    fn from(error: rusoto_core::region::ParseRegionError) -> Self {
        NError::InvalidRegion { e: error }
    }
}

impl From<rusoto_credential::CredentialsError> for NError {
    fn from(error: rusoto_credential::CredentialsError) -> Self {
        NError::InvalidCredentials { e: error }
    }
}

impl From<rusoto_core::request::TlsError> for NError {
    fn from(error: rusoto_core::request::TlsError) -> Self {
        NError::HttpsConnector { e: error }
    }
}
