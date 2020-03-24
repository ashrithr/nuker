pub mod aws;
pub mod config;
mod error;
pub mod graph;
pub mod nuke;
mod resource;
mod service;

type Result<T, E = crate::error::Error> = std::result::Result<T, E>;

#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate derive_more;
