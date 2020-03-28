use crate::{
    aws::{sts::StsService, util},
    config::{GlueConfig, RequiredTags},
    handle_future, handle_future_with_return,
    resource::{EnforcementState, NTag, Resource, ResourceType},
    service::NukerService,
    Result,
};
use async_trait::async_trait;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use rusoto_glue::{
    DeleteDevEndpointRequest, DevEndpoint, GetDevEndpointsRequest, GetTagsRequest, Glue, GlueClient,
};
use tracing::{debug, trace};

#[derive(Clone)]
pub struct GlueService {
    pub client: GlueClient,
    pub config: GlueConfig,
    pub region: Region,
    pub sts_service: StsService,
    pub dry_run: bool,
}

impl GlueService {
    pub fn new(
        profile_name: Option<String>,
        region: Region,
        config: GlueConfig,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = &profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(GlueService {
                client: GlueClient::new_with(HttpClient::new()?, pp, region.clone()),
                config,
                region: region.clone(),
                sts_service: StsService::new(Some(profile.to_string()), region)?,
                dry_run,
            })
        } else {
            Ok(GlueService {
                client: GlueClient::new(region.clone()),
                config,
                region: region.clone(),
                sts_service: StsService::new(None, region)?,
                dry_run,
            })
        }
    }

    async fn get_dev_endpoints(&self) -> Result<Vec<DevEndpoint>> {
        let mut next_token: Option<String> = None;
        let mut dev_endpoints: Vec<DevEndpoint> = Vec::new();

        loop {
            let req = self.client.get_dev_endpoints(GetDevEndpointsRequest {
                next_token,
                ..Default::default()
            });

            if let Ok(result) = handle_future_with_return!(req) {
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
            } else {
                break;
            }
        }

        Ok(dev_endpoints)
    }

    async fn package_endpoints_as_resources(
        &self,
        dev_endpoints: Vec<DevEndpoint>,
    ) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for endpoint in dev_endpoints {
            let endpoint_id = endpoint.endpoint_name.as_ref().unwrap().to_string();
            let ntags = self.get_tags(&endpoint).await?;

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&endpoint_id) {
                    debug!(
                        resource = endpoint_id.as_str(),
                        "Skipping endpoint from ignore list"
                    );
                    EnforcementState::SkipConfig
                } else if endpoint.status != Some("READY".to_string()) {
                    debug!(
                        resource = endpoint_id.as_str(),
                        "Skipping as endpoint is not running"
                    );
                    EnforcementState::SkipStopped
                } else {
                    if self.resource_tags_does_not_match(ntags.clone()) {
                        debug!(
                            resource = endpoint_id.as_str(),
                            "Endpoint tags does not match"
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.resource_allowed_run_time(&endpoint) {
                        debug!(
                            resource = endpoint_id.as_str(),
                            "Endpoint is running beyond allowed runtime ({:?})",
                            self.config.older_than
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else {
                        EnforcementState::Skip
                    }
                }
            };

            resources.push(Resource {
                id: endpoint_id,
                arn: None,
                region: self.region.clone(),
                resource_type: ResourceType::GlueDevEndpoint,
                tags: Some(ntags),
                state: endpoint.status,
                enforcement_state,
                dependencies: None,
            });
        }

        Ok(resources)
    }

    fn resource_allowed_run_time(&self, endpoint: &DevEndpoint) -> bool {
        if self.config.older_than.as_secs() > 0 && endpoint.created_timestamp.is_some() {
            let date = format!("{}", endpoint.created_timestamp.unwrap_or(0f64) as i64);
            util::is_ts_older_than(date.as_str(), &self.config.older_than)
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

    async fn get_tags(&self, endpoint: &DevEndpoint) -> Result<Vec<NTag>> {
        let mut ntags: Vec<NTag> = Vec::new();

        let req = self.client.get_tags(GetTagsRequest {
            resource_arn: format!(
                "arn:aws:glue:{}:{}:devEndpoint/{}",
                self.region.name(),
                self.sts_service.get_account_number().await?,
                endpoint.endpoint_name.as_ref().unwrap()
            ),
        });

        if let Ok(result) = handle_future_with_return!(req) {
            for (key, value) in result.tags.unwrap_or_default() {
                ntags.push(NTag {
                    key: Some(key),
                    value: Some(value),
                })
            }
        }

        Ok(ntags)
    }

    fn check_tags(&self, ntags: Vec<NTag>, required_tags: &Vec<RequiredTags>) -> bool {
        util::compare_tags(Some(ntags), required_tags)
    }

    async fn delete_endpoint(&self, endpoint_name: &str) -> Result<()> {
        debug!(resource = endpoint_name, "Deleting");

        if !self.dry_run {
            let req = self.client.delete_dev_endpoint(DeleteDevEndpointRequest {
                endpoint_name: endpoint_name.into(),
            });
            handle_future!(req);
        }

        Ok(())
    }
}

#[async_trait]
impl NukerService for GlueService {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized Glue resource scanner");

        let dev_endpoints = self.get_dev_endpoints().await?;

        Ok(self.package_endpoints_as_resources(dev_endpoints).await?)
    }

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.delete(resource).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_endpoint(resource.id.as_ref()).await
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
