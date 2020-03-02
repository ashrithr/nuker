use crate::{
    aws::{util, Result},
    config::{GlueConfig, RequiredTags},
    service::{EnforcementState, NTag, NukeService, Resource, ResourceType},
};
use chrono::{TimeZone, Utc};
use log::debug;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use rusoto_glue::{
    DeleteDevEndpointRequest, DevEndpoint, GetDevEndpointsRequest, GetTagsRequest, Glue, GlueClient,
};

pub struct GlueNukeClient {
    pub client: GlueClient,
    pub config: GlueConfig,
    pub region: Region,
    pub account_num: String,
    pub dry_run: bool,
}

impl GlueNukeClient {
    pub fn new(
        profile_name: Option<&str>,
        region: Region,
        config: GlueConfig,
        account_num: String,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(GlueNukeClient {
                client: GlueClient::new_with(HttpClient::new()?, pp, region.clone()),
                config,
                region,
                account_num,
                dry_run,
            })
        } else {
            Ok(GlueNukeClient {
                client: GlueClient::new(region.clone()),
                config,
                region,
                account_num,
                dry_run,
            })
        }
    }

    fn get_dev_endpoints(&self) -> Result<Vec<DevEndpoint>> {
        let mut next_token: Option<String> = None;
        let mut dev_endpoints: Vec<DevEndpoint> = Vec::new();

        loop {
            let result = self
                .client
                .get_dev_endpoints(GetDevEndpointsRequest {
                    next_token,
                    ..Default::default()
                })
                .sync()?;

            if let Some(de) = result.dev_endpoints {
                for e in de {
                    dev_endpoints.push(e);
                }
            }

            if result.next_token.is_none() {
                break;
            } else if result.next_token.is_some()
                && result.next_token.as_ref().clone().unwrap().is_empty()
            {
                break;
            } else {
                next_token = result.next_token;
            }
        }

        Ok(dev_endpoints)
    }

    fn package_endpoints_as_resources(
        &self,
        dev_endpoints: Vec<DevEndpoint>,
    ) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for endpoint in dev_endpoints {
            let endpoint_id = endpoint.endpoint_name.as_ref().unwrap().to_string();
            let ntags = self.get_tags(&endpoint)?;

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&endpoint_id) {
                    debug!("Skipping resource from ignore list - {}", endpoint_id);
                    EnforcementState::SkipConfig
                } else if endpoint.status != Some("READY".to_string()) {
                    debug!("Skipping as resource is not running - {}", endpoint_id);
                    EnforcementState::SkipStopped
                } else {
                    if self.resource_tags_does_not_match(ntags.clone()) {
                        debug!("Resource tags does not match - {}", endpoint_id);
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.resource_allowed_run_time(&endpoint) {
                        debug!(
                            "Resource is running beyond max time ({:?}) - {}",
                            self.config.older_than, endpoint_id
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else {
                        EnforcementState::Skip
                    }
                }
            };

            resources.push(Resource {
                id: endpoint_id,
                region: self.region.clone(),
                resource_type: ResourceType::GlueDevEndpoint,
                tags: Some(ntags),
                state: endpoint.status,
                enforcement_state,
            });
        }

        Ok(resources)
    }

    fn resource_allowed_run_time(&self, endpoint: &DevEndpoint) -> bool {
        if self.config.older_than.as_secs() > 0 && endpoint.created_timestamp.is_some() {
            let endpoint_start = Utc.timestamp(endpoint.created_timestamp.unwrap() as i64, 0);

            let start = Utc::now().timestamp_millis() - self.config.older_than.as_millis() as i64;

            if start > endpoint_start.timestamp_millis() {
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    fn resource_tags_does_not_match(&self, ntags: Vec<NTag>) -> bool {
        if self.config.required_tags.is_some() {
            !self.check_tags(ntags, &self.config.required_tags.as_ref().unwrap())
        } else {
            false
        }
    }

    fn get_tags(&self, endpoint: &DevEndpoint) -> Result<Vec<NTag>> {
        let mut ntags: Vec<NTag> = Vec::new();

        let result = self
            .client
            .get_tags(GetTagsRequest {
                resource_arn: format!(
                    "arn:aws:glue:{}:{}:devEndpoint/{}",
                    self.region.name(),
                    self.account_num,
                    endpoint.endpoint_name.as_ref().unwrap()
                ),
            })
            .sync()?;

        for (key, value) in result.tags.unwrap_or_default() {
            ntags.push(NTag {
                key: Some(key),
                value: Some(value),
            })
        }

        Ok(ntags)
    }

    fn check_tags(&self, ntags: Vec<NTag>, required_tags: &Vec<RequiredTags>) -> bool {
        util::compare_tags(Some(ntags), required_tags)
    }

    fn delete_endpoint(&self, endpoint_name: &str) -> Result<()> {
        debug!("Deleting Glue DevEndpoint - {}", endpoint_name);

        if !self.dry_run {
            self.client
                .delete_dev_endpoint(DeleteDevEndpointRequest {
                    endpoint_name: endpoint_name.into(),
                })
                .sync()?;
        }

        Ok(())
    }
}

impl NukeService for GlueNukeClient {
    fn scan(&self) -> Result<Vec<Resource>> {
        let dev_endpoints = self.get_dev_endpoints()?;

        Ok(self.package_endpoints_as_resources(dev_endpoints)?)
    }

    fn stop(&self, resource: &Resource) -> Result<()> {
        self.delete(resource)
    }

    fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_endpoint(resource.id.as_ref())
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
