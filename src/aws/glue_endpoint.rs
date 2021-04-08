use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_glue::{
    DeleteDevEndpointRequest, DevEndpoint, GetDevEndpointsRequest, GetTagsRequest, Glue, GlueClient,
};
use std::str::FromStr;
use tracing::{debug, trace};

#[derive(Clone)]
pub struct GlueEndpointClient {
    client: GlueClient,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl GlueEndpointClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        GlueEndpointClient {
            client: GlueClient::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
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

    async fn package_resources(&self, dev_endpoints: Vec<DevEndpoint>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for endpoint in dev_endpoints {
            let endpoint_id = endpoint.endpoint_name.as_ref().unwrap();
            let arn = format!(
                "arn:aws:glue:{}:{}:devEndpoint/{}",
                self.region.name(),
                self.account_num,
                endpoint_id
            );
            let ntags = self.get_tags(&arn).await?;

            resources.push(Resource {
                id: endpoint.endpoint_name.unwrap(),
                arn: Some(arn),
                type_: ClientType::GlueEndpoint,
                region: self.region.clone(),
                tags: Some(ntags),
                state: ResourceState::from_str(endpoint.status.unwrap_or_default().as_str()).ok(),
                start_time: Some(format!(
                    "{}",
                    endpoint.created_timestamp.unwrap_or(0f64) as i64
                )),
                enforcement_state: EnforcementState::SkipUnknownState,
                enforcement_reason: None,
                resource_type: endpoint.worker_type.map(|t| vec![t]),
                dependencies: None,
                termination_protection: None,
            });
        }

        Ok(resources)
    }

    async fn get_tags(&self, arn: &str) -> Result<Vec<NTag>> {
        let mut ntags: Vec<NTag> = Vec::new();

        let req = self.client.get_tags(GetTagsRequest {
            resource_arn: arn.to_string(),
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
impl NukerClient for GlueEndpointClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized Glue resource scanner");

        let dev_endpoints = self.get_dev_endpoints().await?;
        Ok(self.package_resources(dev_endpoints).await?)
    }

    async fn dependencies(&self, _resource: &Resource) -> Option<Vec<Resource>> {
        None
    }

    async fn additional_filters(
        &self,
        _resource: &Resource,
        _config: &ResourceConfig,
    ) -> Option<bool> {
        None
    }

    async fn stop(&self, _resource: &Resource) -> Result<()> {
        Ok(())
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_endpoint(resource.id.as_ref()).await
    }
}
