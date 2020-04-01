//! Represents a Nuker Client
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, Resource, ResourceState, ResourceType};
use crate::CwClient;
use crate::Event;
use crate::NSender;
use async_trait::async_trait;
use dyn_clone::DynClone;
use std::sync::Arc;

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
pub trait ResourceFilter {
    /// Checks to see if the required tags for a particular resource exists or
    /// not.
    fn filter_by_tags(&self, resource: &Resource, config: &ResourceConfig) -> bool {
        if let Some(ref rt) = config.required_tags {
            let tags = resource.tags.clone();
            crate::util::compare_tags(tags, rt)
        } else {
            false
        }
    }

    /// Filters a resource based on its start time
    fn filter_by_runtime(&self, resource: &Resource, config: &ResourceConfig) -> bool {
        if let (Some(ref older_than), Some(st)) =
            (config.max_run_time, resource.start_time.as_deref())
        {
            crate::util::is_ts_older_than(st, older_than)
        } else {
            false
        }
    }

    /// Filters a resource based on its type and types that are allowed
    fn filter_by_allowed_types(&self, resource: &Resource, config: &ResourceConfig) -> bool {
        if let (Some(allowed), Some(type_)) = (
            config.allowed_types.as_deref(),
            resource.resource_type.as_ref(),
        ) {
            allowed.contains(type_)
        } else {
            false
        }
    }

    fn filter_by_whitelist(&self, resource: &Resource, config: &ResourceConfig) -> bool {
        if let Some(ref whitelist) = config.whitelist {
            if whitelist.contains(&resource.id) {
                return true;
            }
        }
        false
    }

    /// Filter resources that are not running
    fn filter_by_state(&self, resource: &Resource) -> bool {
        if let Some(ref state) = resource.state {
            match state {
                ResourceState::Running => false,
                _ => true,
            }
        } else {
            false
        }
    }

    async fn filter_by_idle_rules(
        &self,
        resource: &Resource,
        config: &ResourceConfig,
        cw_client: Arc<Box<CwClient>>,
    ) -> bool {
        if let Some(ref _rules) = config.idle_rules {
            match resource.type_ {
                ResourceType::Ec2Instance => cw_client.filter_instance(resource.id.as_str()).await,
                _ => false,
            }
        } else {
            false
        }
    }

    /// Additional filters to apply
    fn additional_filters(&self, resource: &Resource, config: &ResourceConfig) -> bool;

    /// Filters a provided resource using the available filters
    async fn filter_resource(
        &self,
        resource: &Resource,
        config: &ResourceConfig,
        cw_client: Arc<Box<CwClient>>,
    ) -> EnforcementState {
        if self.filter_by_whitelist(resource, config) {
            EnforcementState::SkipConfig
        } else if self.filter_by_runtime(resource, config) {
            EnforcementState::from_target_state(&config.target_state)
        } else if self.filter_by_state(resource) {
            EnforcementState::SkipStopped
        } else if self.filter_by_tags(resource, config) {
            EnforcementState::from_target_state(&config.target_state)
        } else if self.filter_by_idle_rules(resource, config, cw_client).await {
            EnforcementState::from_target_state(&config.target_state)
        } else if self.additional_filters(resource, config) {
            EnforcementState::from_target_state(&config.target_state)
        } else {
            EnforcementState::Skip
        }
    }
}

#[async_trait]
pub trait ResourceCleaner {
    async fn cleanup(&self, resource: &Resource);
}

pub trait NukerClient:
    ResourceScanner + ResourceCleaner + ResourceFilter + Send + Sync + DynClone
{
}

dyn_clone::clone_trait_object!(NukerClient);
