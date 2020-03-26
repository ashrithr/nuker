pub mod aws;
pub mod config;
mod error;
pub mod graph;
pub mod nuke;
mod resource;
mod service;

type Result<T, E = failure::Error> = std::result::Result<T, E>;
