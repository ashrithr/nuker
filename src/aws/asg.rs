use crate::aws::{util, Result};
use crate::config::{AutoScalingConfig, RequiredTags};
use crate::resource::{EnforcementState, NTag, Resource, ResourceType};
use crate::service::NukerService;
use async_trait::async_trait;
use rusoto_autoscaling::{
    AutoScalingGroup, AutoScalingGroupNamesType, Autoscaling, AutoscalingClient,
    DeleteAutoScalingGroupType, TagDescription,
};
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use tracing::{debug, trace};

#[derive(Clone)]
pub struct AsgService {
    pub client: AutoscalingClient,
    pub config: AutoScalingConfig,
    pub region: Region,
    pub dry_run: bool,
}

impl AsgService {
    pub fn new(
        profile_name: Option<String>,
        region: Region,
        config: AutoScalingConfig,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = &profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(AsgService {
                client: AutoscalingClient::new_with(HttpClient::new()?, pp, region.clone()),
                config,
                region,
                dry_run,
            })
        } else {
            Ok(AsgService {
                client: AutoscalingClient::new(region.clone()),
                config,
                region,
                dry_run,
            })
        }
    }

    async fn package_asgs_as_resources(
        &self,
        asgs: Vec<AutoScalingGroup>,
    ) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for asg in asgs {
            let asg_name = asg.auto_scaling_group_name.clone();
            let tags = asg.tags.clone();

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&asg_name) {
                    debug!(
                        resource = asg_name.as_str(),
                        "Skipping resource from ignore list"
                    );
                    EnforcementState::SkipConfig
                } else {
                    if self.resource_tags_does_not_match(&tags).await {
                        debug!(resource = asg_name.as_str(), "ASG tags does not match");
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.is_resource_idle(&asg) {
                        debug!(resource = asg_name.as_str(), "ASG is idle");
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else {
                        EnforcementState::Skip
                    }
                }
            };

            resources.push(Resource {
                id: asg_name,
                arn: asg.auto_scaling_group_arn,
                resource_type: ResourceType::Asg,
                region: self.region.clone(),
                tags: self.package_tags_as_ntags(tags),
                state: asg.status,
                enforcement_state,
                dependencies: None,
            });
        }

        Ok(resources)
    }

    async fn resource_tags_does_not_match(&self, tags: &Option<Vec<TagDescription>>) -> bool {
        if self.config.required_tags.is_some() {
            !self.check_tags(tags, &self.config.required_tags.as_ref().unwrap())
        } else {
            false
        }
    }

    async fn get_asgs(&self) -> Result<Vec<AutoScalingGroup>> {
        let mut asgs: Vec<AutoScalingGroup> = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let result = self
                .client
                .describe_auto_scaling_groups(AutoScalingGroupNamesType {
                    next_token,
                    ..Default::default()
                })
                .await?;

            for asg in result.auto_scaling_groups {
                asgs.push(asg);
            }

            if result.next_token.is_none() {
                break;
            } else {
                next_token = result.next_token;
            }
        }

        Ok(asgs)
    }

    async fn delete_asg(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting");

        if !self.dry_run {
            self.client
                .delete_auto_scaling_group(DeleteAutoScalingGroupType {
                    auto_scaling_group_name: resource.id.clone(),
                    force_delete: Some(true),
                })
                .await?;
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

    fn check_tags(
        &self,
        tags: &Option<Vec<TagDescription>>,
        required_tags: &Vec<RequiredTags>,
    ) -> bool {
        let ntags = self.package_tags_as_ntags(tags.to_owned());
        util::compare_tags(ntags, required_tags)
    }

    fn package_tags_as_ntags(&self, tags: Option<Vec<TagDescription>>) -> Option<Vec<NTag>> {
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
impl NukerService for AsgService {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized ASG resource scanner");
        let asgs = self.get_asgs().await?;

        Ok(self.package_asgs_as_resources(asgs).await?)
    }

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.delete_asg(resource).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_asg(resource).await
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
