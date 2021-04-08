use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_elbv2::{
    DeleteLoadBalancerInput, DescribeLoadBalancersInput, DescribeTagsInput, DescribeTagsOutput,
    Elb, ElbClient, LoadBalancer, Tag,
};
use std::str::FromStr;
use tracing::{debug, trace};

const ALB_TYPE: &str = "application";
const NLB_TYPE: &str = "network";

#[derive(Clone)]
pub struct ElbNlbClient {
    client: ElbClient,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl ElbNlbClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        ElbNlbClient {
            client: ElbClient::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn package_resources(&self, lbs: Vec<LoadBalancer>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for lb in lbs {
            let lb_arn = lb.load_balancer_arn.as_ref().unwrap();
            let tags = self.list_tags(lb_arn).await;

            resources.push(Resource {
                id: lb.load_balancer_name.unwrap(),
                arn: lb.load_balancer_arn,
                type_: ClientType::ElbNlb,
                region: self.region.clone(),
                tags: self.package_tags_as_ntags(tags),
                state: ResourceState::from_str(lb.state.as_ref().unwrap().code.as_ref().unwrap())
                    .ok(),
                start_time: lb.created_time,
                enforcement_state: EnforcementState::SkipUnknownState,
                enforcement_reason: None,
                resource_type: None,
                dependencies: None,
                termination_protection: None,
            });
        }

        Ok(resources)
    }

    async fn list_tags(&self, arn: &str) -> Option<Vec<Tag>> {
        let req = self.client.describe_tags(DescribeTagsInput {
            resource_arns: vec![arn.to_string()],
        });

        handle_future_with_return!(req)
            .and_then(|mut result: DescribeTagsOutput| {
                Ok(result
                    .tag_descriptions
                    .as_mut()
                    .unwrap()
                    .pop()
                    .unwrap_or_default()
                    .tags)
            })
            .ok()
            .unwrap_or_default()
    }

    fn package_tags_as_ntags(&self, tags: Option<Vec<Tag>>) -> Option<Vec<NTag>> {
        tags.map(|ts| {
            ts.iter()
                .map(|tag| NTag {
                    key: Some(tag.key.clone()),
                    value: tag.value.clone(),
                })
                .collect()
        })
    }

    async fn get_load_balancers(&self) -> Result<Vec<LoadBalancer>> {
        let mut next_token: Option<String> = None;
        let mut lbs: Vec<LoadBalancer> = Vec::new();

        loop {
            let req = self
                .client
                .describe_load_balancers(DescribeLoadBalancersInput {
                    marker: next_token,
                    ..Default::default()
                });

            if let Ok(result) = handle_future_with_return!(req) {
                if let Some(load_balancers) = result.load_balancers {
                    for lb in load_balancers {
                        let resource_type = match lb.type_.as_ref().unwrap().as_str() {
                            ALB_TYPE => Some(ClientType::ElbAlb),
                            NLB_TYPE => Some(ClientType::ElbNlb),
                            _ => None,
                        };

                        if resource_type == Some(ClientType::ElbNlb) {
                            lbs.push(lb);
                        }
                    }
                }

                if result.next_marker.is_none() {
                    break;
                } else {
                    next_token = result.next_marker;
                }
            } else {
                break;
            }
        }

        Ok(lbs)
    }

    async fn delete_load_balancer(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting");

        if !self.dry_run && resource.arn.is_some() {
            let req = self.client.delete_load_balancer(DeleteLoadBalancerInput {
                load_balancer_arn: resource.arn.clone().unwrap(),
                ..Default::default()
            });
            handle_future!(req);
        }

        Ok(())
    }
}

#[async_trait]
impl NukerClient for ElbNlbClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized ELB ALB resource scanner");
        let lbs = self.get_load_balancers().await?;

        Ok(self.package_resources(lbs).await?)
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
        self.delete_load_balancer(resource).await
    }
}
