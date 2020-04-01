mod aws;
mod config;
mod error;
mod graph;
#[macro_use]
mod macros;
mod client;
mod event;
mod nuke;
mod resource;
mod service;

pub use config::parse_args;
pub use config::parse_config_file;
pub use error::NError as Error;
pub use event::Event;
pub use macros::*;
pub use nuke::Nuker;
use std::result::Result as StdResult;
use tokio::sync::mpsc::Receiver as NReceiver;
use tokio::sync::mpsc::Sender as NSender;

pub type Result<T> = StdResult<T, Error>;
