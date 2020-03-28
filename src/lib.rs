mod aws;
mod config;
mod error;
mod graph;
#[macro_use]
mod macros;
mod nuke;
mod resource;
mod service;

pub use config::parse_args;
pub use config::parse_config_file;
pub use error::NError as Error;
pub use macros::*;
pub use nuke::Nuker;
use std::result::Result as StdResult;

pub type Result<T> = StdResult<T, Error>;
