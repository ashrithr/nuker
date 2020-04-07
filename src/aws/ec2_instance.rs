use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_ec2::{
    AttributeBooleanValue, DescribeInstanceAttributeRequest, DescribeInstancesRequest, Ec2,
    Ec2Client, Instance, ModifyInstanceAttributeRequest, StopInstancesRequest, Tag,
    TerminateInstancesRequest,
};
use std::str::FromStr;
use tracing::{debug, trace};

#[derive(Clone)]
pub struct Ec2InstanceClient {
    client: Ec2Client,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl Ec2InstanceClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        Ec2InstanceClient {
            client: Ec2Client::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn package_resources(&self, instances: Vec<Instance>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for instance in instances {
            let instance_id = instance.instance_id.unwrap();
            let arn = format!(
                "arn:aws:ec2:{}:{}:instance/{}",
                self.region.name(),
                self.account_num,
                instance_id
            );
            let mut termination_protection: Option<bool> = None;

            if let Some(ref tp) = self.config.termination_protection {
                if tp.ignore {
                    termination_protection = self
                        .check_termination_protection(instance_id.as_str())
                        .await;
                }
            }

            resources.push(Resource {
                id: instance_id,
                arn: Some(arn),
                type_: ClientType::Ec2Instance,
                region: self.region.clone(),
                tags: self.package_tags(instance.tags),
                state: ResourceState::from_str(
                    instance
                        .state
                        .as_ref()
                        .unwrap()
                        .name
                        .as_deref()
                        .unwrap_or_default(),
                )
                .ok(),
                start_time: instance.launch_time,
                enforcement_state: EnforcementState::SkipUnknownState,
                resource_type: instance.instance_type.map(|t| vec![t]),
                dependencies: None,
                termination_protection,
            });
        }

        Ok(resources)
    }

    async fn get_instances(&self) -> Result<Vec<Instance>> {
        let mut next_token: Option<String> = None;
        let mut instances: Vec<Instance> = Vec::new();

        loop {
            let req = self.client.describe_instances(DescribeInstancesRequest {
                dry_run: None,
                filters: None,
                instance_ids: None,
                max_results: None,
                next_token,
            });

            if let Ok(result) = handle_future_with_return!(req) {
                if let Some(reservations) = result.reservations {
                    let reservations: Vec<Vec<Instance>> = reservations
                        .into_iter()
                        .filter_map(|reservation| reservation.instances)
                        .collect();

                    let mut temp_instances: Vec<Instance> =
                        reservations.into_iter().flatten().collect();

                    instances.append(&mut temp_instances);
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

        Ok(instances)
    }

    async fn check_termination_protection(&self, instance_id: &str) -> Option<bool> {
        let mut termination_protection: Option<bool> = None;

        let req = self
            .client
            .describe_instance_attribute(DescribeInstanceAttributeRequest {
                attribute: "disableApiTermination".into(),
                instance_id: instance_id.into(),
                ..Default::default()
            });

        if let Ok(resp) = handle_future_with_return!(req) {
            if resp.disable_api_termination.unwrap().value.unwrap() {
                termination_protection = Some(true)
            }
        }

        termination_protection
    }

    async fn disable_termination_protection(&self, instance_id: &str) -> Result<()> {
        debug!(
            "Terminating protection was enabled for: {}. Trying to Disable it.",
            instance_id
        );

        let req = self
            .client
            .modify_instance_attribute(ModifyInstanceAttributeRequest {
                disable_api_termination: Some(AttributeBooleanValue { value: Some(false) }),
                instance_id: instance_id.into(),
                ..Default::default()
            });

        handle_future!(req);

        Ok(())
    }

    async fn stop_instance(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Stopping");

        if !self.dry_run {
            let req = self.client.stop_instances(StopInstancesRequest {
                instance_ids: vec![resource.id.clone()],
                force: Some(true),
                ..Default::default()
            });
            handle_future!(req);
        }

        Ok(())
    }

    async fn delete_instance(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting.");

        if !self.dry_run {
            if let Some(tp_enabled) = resource.termination_protection {
                if tp_enabled {
                    self.disable_termination_protection(resource.id.as_ref())
                        .await?;
                }
            }

            let req = self.client.terminate_instances(TerminateInstancesRequest {
                instance_ids: vec![resource.id.clone()],
                ..Default::default()
            });
            handle_future!(req);
        }

        Ok(())
    }

    fn package_tags(&self, tags: Option<Vec<Tag>>) -> Option<Vec<NTag>> {
        tags.map(|ts| {
            ts.into_iter()
                .map(|mut tag| NTag {
                    key: std::mem::replace(&mut tag.key, None),
                    value: std::mem::replace(&mut tag.value, None),
                })
                .collect()
        })
    }
}

#[async_trait]
impl NukerClient for Ec2InstanceClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized EC2 Instance resource scanner");
        let instances = self.get_instances().await?;
        Ok(self.package_resources(instances).await?)
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

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.stop_instance(resource).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_instance(resource).await
    }
}
