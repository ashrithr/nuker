use crate::aws::cloudwatch::CwClient;
use crate::aws::util;
use crate::config::{ElbConfig, RequiredTags};
use crate::resource::{EnforcementState, NTag, Resource, ResourceType};
use crate::service::NukerService;
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use rusoto_elbv2::{
    DeleteLoadBalancerInput, DescribeLoadBalancersInput, DescribeTagsInput, DescribeTagsOutput,
    Elb, ElbClient, LoadBalancer, Tag,
};
use std::sync::Arc;
use tracing::{debug, trace};

const ALB_TYPE: &str = "application";
const NLB_TYPE: &str = "network";

#[derive(Clone)]
pub struct ElbService {
    pub client: ElbClient,
    pub cw_client: Arc<Box<CwClient>>,
    pub config: ElbConfig,
    pub region: Region,
    pub dry_run: bool,
}

impl ElbService {
    pub fn new(
        profile_name: Option<String>,
        region: Region,
        config: ElbConfig,
        cw_client: Arc<Box<CwClient>>,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = &profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(ElbService {
                client: ElbClient::new_with(HttpClient::new()?, pp, region.clone()),
                cw_client,
                config,
                region,
                dry_run,
            })
        } else {
            Ok(ElbService {
                client: ElbClient::new(region.clone()),
                cw_client,
                config,
                region,
                dry_run,
            })
        }
    }

    async fn package_load_balancers_as_resources(
        &self,
        lbs: Vec<LoadBalancer>,
    ) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for lb in lbs {
            let lb_name = lb.load_balancer_name.as_ref().unwrap().clone();
            let lb_arn = lb.load_balancer_arn.as_ref().unwrap().clone();
            let tags = self.list_tags(&lb).await;
            let state = lb.state.as_ref().unwrap().clone().code;

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&lb_name) {
                    debug!(
                        resource = lb_name.as_str(),
                        "Skipping resource from ignore list"
                    );
                    EnforcementState::SkipConfig
                } else if state != Some("active".to_string()) {
                    EnforcementState::SkipStopped
                } else {
                    if self.resource_tags_does_not_match(&tags).await {
                        debug!(
                            resource = lb_name.as_str(),
                            "Load balancer tags does not match"
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.is_resource_idle(&lb).await {
                        debug!(resource = lb_name.as_str(), "Load Balancer is idle");
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else {
                        EnforcementState::Skip
                    }
                }
            };

            let resource_type = match lb.type_.as_ref().unwrap().as_str() {
                ALB_TYPE => Some(ResourceType::ElbAlb),
                NLB_TYPE => Some(ResourceType::ElbNlb),
                _ => None,
            };

            resources.push(Resource {
                id: lb_name,
                arn: Some(lb_arn),
                resource_type: resource_type.unwrap(),
                region: self.region.clone(),
                tags: self.package_tags_as_ntags(tags),
                state,
                enforcement_state,
                dependencies: None,
            });
        }

        Ok(resources)
    }

    async fn resource_tags_does_not_match(&self, tags: &Option<Vec<Tag>>) -> bool {
        if self.config.required_tags.is_some() {
            !self.check_tags(tags, &self.config.required_tags.as_ref().unwrap())
        } else {
            false
        }
    }

    async fn list_tags(&self, lb: &LoadBalancer) -> Option<Vec<Tag>> {
        let arn = lb.load_balancer_arn.as_ref().unwrap().clone();
        let req = self.client.describe_tags(DescribeTagsInput {
            resource_arns: vec![arn],
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

    async fn is_resource_idle(&self, lb: &LoadBalancer) -> bool {
        let dimension_value = format!(
            "app/{}/{}",
            lb.load_balancer_name.as_ref().unwrap().clone(),
            lb.load_balancer_arn
                .as_ref()
                .unwrap()
                .clone()
                .split('/')
                .last()
                .unwrap()
        );

        match lb.type_.as_ref().unwrap().as_str() {
            ALB_TYPE => {
                if self.config.alb_idle_rules.is_some() {
                    !self
                        .cw_client
                        .filter_alb_load_balancer(&dimension_value)
                        .await
                } else {
                    false
                }
            }
            NLB_TYPE => {
                if self.config.nlb_idle_rules.is_some() {
                    !self
                        .cw_client
                        .filter_nlb_load_balancer(&dimension_value)
                        .await
                } else {
                    false
                }
            }
            _ => false,
        }
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

    fn check_tags(&self, tags: &Option<Vec<Tag>>, required_tags: &Vec<RequiredTags>) -> bool {
        let ntags = self.package_tags_as_ntags(tags.to_owned());
        util::compare_tags(ntags, required_tags)
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
                        lbs.push(lb);
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

    async fn delete_load_balancer(&self, lb_arn: Option<String>) -> Result<()> {
        debug!(resource = lb_arn.as_ref().unwrap().as_str(), "Deleting");

        if !self.dry_run && lb_arn.is_some() {
            let req = self.client.delete_load_balancer(DeleteLoadBalancerInput {
                load_balancer_arn: lb_arn.unwrap(),
                ..Default::default()
            });
            handle_future!(req);
        }

        Ok(())
    }
}

#[async_trait]
impl NukerService for ElbService {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized ELB resource scanner");
        let lbs = self.get_load_balancers().await?;

        Ok(self.package_load_balancers_as_resources(lbs).await?)
    }

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.delete_load_balancer(resource.arn.clone()).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_load_balancer(resource.arn.clone()).await
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
