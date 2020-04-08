// #![feature(const_fn)]

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
mod util;

pub use aws::CwClient;
pub use config::parse_args;
pub use config::parse_config_file;
pub use error::NError as Error;
pub use event::Event;
pub use macros::*;
pub use nuke::Nuker;
use std::error::Error as StdError;
use std::result::Result as StdResult;
use tokio::sync::mpsc::Sender as NSender;
pub use util::print_type_of;

pub type Result<T> = StdResult<T, Error>;
