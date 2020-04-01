use crate::aws::{ClientDetails, CwClient};
use crate::client::{NukerClient, ResourceCleaner, ResourceFilter, ResourceScanner};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, Resource, ResourceState, ResourceType};
use crate::Event;
use crate::NSender;
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::{Client, Region};
use rusoto_ec2::{
    Address, AttributeBooleanValue, DeleteNetworkInterfaceRequest, DeleteSecurityGroupRequest,
    DescribeAddressesRequest, DescribeInstanceAttributeRequest, DescribeInstancesRequest,
    DescribeNetworkInterfaceAttributeRequest, DescribeNetworkInterfacesRequest,
    DescribeSecurityGroupsRequest, DetachNetworkInterfaceRequest, Ec2, Ec2Client, Filter, Instance,
    ModifyInstanceAttributeRequest, NetworkInterface, ReleaseAddressRequest, StopInstancesRequest,
    Tag, TerminateInstancesRequest,
};
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, trace, warn};

const RUNNING_STATE: i64 = 16;
const STOPPED_STATE: i64 = 80;

#[derive(Clone)]
pub struct Ec2Instance {
    client: Ec2Client,
    region: Region,
    account_num: String,
    cw_client: Arc<Box<CwClient>>,
    config: ResourceConfig,
    dry_run: bool,
}

impl Ec2Instance {
    pub fn new(
        cd: &ClientDetails,
        config: ResourceConfig,
        cw_client: Arc<Box<CwClient>>,
        dry_run: bool,
    ) -> Self {
        Ec2Instance {
            client: Ec2Client::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            cw_client,
            config,
            dry_run,
        }
    }

    async fn package_resources(&self, instances: Vec<Instance>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for instance in instances {
            let instance_id = instance.instance_id.as_ref().unwrap();

            resources.push(Resource {
                id: instance_id.to_owned(),
                arn: Some(format!(
                    "arn:aws:ec2:{}:{}:instance/{}",
                    self.region.name(),
                    self.account_num,
                    instance_id
                )),
                type_: ResourceType::Ec2Instance,
                region: self.region.clone(),
                tags: None, //self.package_tags(instance.tags),
                state: Some(ResourceState::from_str(
                    instance.state.as_ref().unwrap().name.as_deref().unwrap(),
                )?),
                start_time: None, //instance.start_time,
                enforcement_state: EnforcementState::SkipUnknownState,
                resource_type: instance.instance_type,
                dependencies: None,
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

    async fn disable_termination_protection(&self, instance_id: &str) -> Result<()> {
        let req = self
            .client
            .describe_instance_attribute(DescribeInstanceAttributeRequest {
                attribute: "disableApiTermination".into(),
                instance_id: instance_id.into(),
                ..Default::default()
            });

        if let Ok(resp) = handle_future_with_return!(req) {
            if resp.disable_api_termination.unwrap().value.unwrap() {
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
            }
        }

        Ok(())
    }

    async fn stop_instance(&self, resource: &Resource) -> Result<()> {
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
        if !self.dry_run {
            if let Some(ref termination_protection) = self.config.termination_protection {
                if termination_protection.ignore {
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
}

#[async_trait]
impl ResourceScanner for Ec2Instance {
    async fn scan(&self) -> Result<Vec<Resource>> {
        let instances = self.get_instances().await?;
        Ok(self.package_resources(instances).await?)
    }

    async fn dependencies(&self, resource: &Resource) -> Option<Vec<Resource>> {
        None
    }

    async fn publish(&self, mut tx: NSender<Event>) {
        unimplemented!()
    }
}

#[async_trait]
impl ResourceCleaner for Ec2Instance {
    async fn cleanup(&self, resource: &Resource) {
        unimplemented!()
    }
}

#[async_trait]
impl ResourceFilter for Ec2Instance {
    fn additional_filters(&self, resource: &Resource, config: &ResourceConfig) -> bool {
        false
    }
}

impl NukerClient for Ec2Instance {}
