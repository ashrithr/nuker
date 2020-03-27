use rusoto_core::{request::BufferedHttpResponse, RusotoError};
use serde::Deserialize;
use tracing::debug;

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

#[derive(Debug, PartialEq)]
pub enum Error<E> {
    Rusoto(RusotoError<E>),
    Throttling(String),
    Validation(String),
}

impl<E> From<RusotoError<E>> for Error<E> {
    fn from(err: RusotoError<E>) -> Self {
        match &err {
            RusotoError::Unknown(BufferedHttpResponse { ref body, .. }) => {
                if let Ok(ErrorResponse { error }) =
                    serde_xml_rs::from_reader::<_, ErrorResponse>(body.as_ref())
                {
                    match error.code.as_str() {
                        "Throttling" => return Error::Throttling(error.message),
                        "ValidationError" => return Error::Validation(error.message),
                        code => debug!("unmatched error code {}", code),
                    }
                }
                Error::Rusoto(err)
            }
            _ => Error::Rusoto(err),
        }
    }
}
