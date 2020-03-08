//! Represents a Nukable service
use crate::resource::{EnforcementState, Resource};
use async_trait::async_trait;
use dyn_clone::DynClone;
use std::any::Any;

type Result<T, E = crate::error::Error> = std::result::Result<T, E>;

#[allow(dead_code)]
pub enum FilterRule {
    /// A filter rule that checks if the required tags are provided
    /// for a given resource
    RequiredTags,
    /// A filter rule that checks to see if the resource falls under
    /// Idle (no usage)
    Idle,
    /// A filter rule that checks to see if the resource is using
    /// allowed type of the resource
    AllowedTypes,
}

#[async_trait]
pub trait NukerService: Any + Send + Sync + DynClone {
    /// Get all the resources without applying any filters
    async fn scan(&self) -> Result<Vec<Resource>>;

    /// Clean up the resources, based on the enforcement state figured out
    /// from the specified rules
    async fn cleanup(&self, resources: Vec<Resource>) -> Result<()> {
        for resource in resources {
            match resource.enforcement_state {
                EnforcementState::Stop => self.stop(&resource).await?,
                EnforcementState::Delete => self.delete(&resource).await?,
                _ => {}
            }
        }

        Ok(())
    }

    /// Stop the resource
    async fn stop(&self, resource: &Resource) -> Result<()>;

    /// Delete the resource
    async fn delete(&self, resource: &Resource) -> Result<()>;

    fn as_any(&self) -> &dyn ::std::any::Any;
}

dyn_clone::clone_trait_object!(NukerService);

pub trait RequiredTagsFilter {
    fn filter(&self) -> Result<Vec<Resource>>;
}

pub trait AllowedTypesFilter {
    fn filter(&self) -> Result<Vec<Resource>>;
}

pub trait IdleFilter {
    fn filter(&self) -> Result<Vec<Resource>>;
}
