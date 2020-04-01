//! Represents a Nuker Client
use crate::resource::{EnforcementState, Resource};
use crate::Event;
use crate::NSender;
use crate::Result;
use crate::StdResult;
use async_trait::async_trait;
use dyn_clone::DynClone;
use std::{
    any::Any,
    error::Error,
    fmt::{Display, Error as FmtError, Formatter},
    hash::Hash,
    str::FromStr,
};

#[derive(Debug, PartialEq, Hash)]
pub enum ClientType {
    Ec2Instance,
    Ec2Sg,
    RdsInstance,
    RdsCluster,
    Root,
}

#[async_trait]
pub trait ResourceScanner {
    /// Scans for all the resources that are scannable by a Resource Scanner
    /// before applying any Filter's and Rule's
    async fn scan(&self) -> Vec<Resource>;

    /// Find dependent resources for a given Resource
    async fn dependencies(&self, resource: &Resource) -> Option<Vec<Resource>>;

    /// Publishes the resources to shared Channel
    async fn publish(&self, mut tx: NSender<Event>);
}

#[async_trait]
pub trait ResourceCleaner {
    async fn cleanup(&self, resource: &Resource);
}

pub trait NukerClient: ResourceScanner + ResourceCleaner + Send + Sync + DynClone {}

dyn_clone::clone_trait_object!(NukerClient);
