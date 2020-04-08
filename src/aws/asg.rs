use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_autoscaling::{
    AutoScalingGroup, AutoScalingGroupNamesType, Autoscaling, AutoscalingClient,
    DeleteAutoScalingGroupType, TagDescription,
};
use rusoto_core::Region;
use std::str::FromStr;
use tracing::{debug, trace};

#[derive(Clone)]
pub struct AsgClient {
    pub client: AutoscalingClient,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl AsgClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        AsgClient {
            client: AutoscalingClient::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn package_resources(&self, asgs: Vec<AutoScalingGroup>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for asg in asgs {
            resources.push(Resource {
                id: asg.auto_scaling_group_name,
                arn: asg.auto_scaling_group_arn,
                type_: ClientType::Asg,
                region: self.region.clone(),
                tags: self.package_tags(asg.tags),
                state: Some(ResourceState::from_str(asg.status.as_ref().unwrap()).unwrap()),
                start_time: Some(asg.created_time),
                enforcement_state: EnforcementState::SkipUnknownState,
                resource_type: None,
                dependencies: None,
                termination_protection: None,
            });
        }

        Ok(resources)
    }

    async fn get_asgs(&self, filter: Option<Vec<String>>) -> Vec<AutoScalingGroup> {
        let mut asgs: Vec<AutoScalingGroup> = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let req = self
                .client
                .describe_auto_scaling_groups(AutoScalingGroupNamesType {
                    auto_scaling_group_names: filter.clone(),
                    next_token,
                    ..Default::default()
                });

            if let Ok(result) = handle_future_with_return!(req) {
                for asg in result.auto_scaling_groups {
                    asgs.push(asg);
                }

                if result.next_token.is_none() {
                    break;
                } else {
                    next_token = result.next_token;
                }
            } else {
                break;
            }
        }

        asgs
    }

    async fn delete_asg(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting");

        if !self.dry_run {
            let req = self
                .client
                .delete_auto_scaling_group(DeleteAutoScalingGroupType {
                    auto_scaling_group_name: resource.id.clone(),
                    force_delete: Some(true),
                });
            handle_future!(req);
        }

        Ok(())
    }

    fn is_resource_idle(&self, asg: &AutoScalingGroup) -> bool {
        if asg.instances.is_some() && asg.instances.as_ref().unwrap().len() > 0 {
            if asg.load_balancer_names.is_some()
                && asg.load_balancer_names.as_ref().unwrap().len() > 0
            {
                return true;
            }
        }

        false
    }

    fn package_tags(&self, tags: Option<Vec<TagDescription>>) -> Option<Vec<NTag>> {
        tags.map(|ts| {
            ts.iter()
                .map(|tag| NTag {
                    key: tag.key.clone(),
                    value: tag.value.clone(),
                })
                .collect()
        })
    }
}

#[async_trait]
impl NukerClient for AsgClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized ASG resource scanner");
        let asgs = self.get_asgs(None).await;

        Ok(self.package_resources(asgs).await?)
    }

    async fn dependencies(&self, _resource: &Resource) -> Option<Vec<Resource>> {
        None
    }

    async fn additional_filters(
        &self,
        resource: &Resource,
        _config: &ResourceConfig,
    ) -> Option<bool> {
        let mut asg = self.get_asgs(Some(vec![resource.id.clone()])).await;

        Some(self.is_resource_idle(asg.pop().as_ref().unwrap()))
    }

    async fn stop(&self, _resource: &Resource) -> Result<()> {
        Ok(())
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_asg(resource).await
    }
}
