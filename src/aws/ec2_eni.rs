use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_ec2::{
    DeleteNetworkInterfaceRequest, DescribeNetworkInterfaceAttributeRequest,
    DescribeNetworkInterfacesRequest, DetachNetworkInterfaceRequest, Ec2, Ec2Client,
    NetworkInterface, Tag,
};
use std::str::FromStr;
use tracing::{debug, trace, warn};

#[derive(Clone)]
pub struct Ec2EniClient {
    client: Ec2Client,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl Ec2EniClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        Ec2EniClient {
            client: Ec2Client::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn package_resources(&self, mut enis: Vec<NetworkInterface>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for eni in &mut enis {
            let arn = format!(
                "arn:aws:ec2:{}:{}:network-interface/{}",
                self.region.name(),
                self.account_num,
                eni.network_interface_id.as_ref().unwrap(),
            );

            resources.push(Resource {
                id: eni.network_interface_id.take().unwrap(),
                arn: Some(arn),
                type_: ClientType::Ec2Eni,
                region: self.region.clone(),
                tags: self.package_tags(eni.tag_set.take()),
                state: ResourceState::from_str(eni.status.as_deref().unwrap_or_default()).ok(),
                start_time: None,
                enforcement_state: EnforcementState::SkipUnknownState,
                resource_type: None,
                dependencies: None,
                termination_protection: None,
            });
        }

        Ok(resources)
    }

    async fn get_enis(&self) -> Result<Vec<NetworkInterface>> {
        let mut next_token: Option<String> = None;
        let mut interfaces: Vec<NetworkInterface> = Vec::new();

        loop {
            let req = self
                .client
                .describe_network_interfaces(DescribeNetworkInterfacesRequest {
                    next_token,
                    ..Default::default()
                });

            if let Ok(result) = handle_future_with_return!(req) {
                if let Some(nics) = result.network_interfaces {
                    for nic in nics {
                        interfaces.push(nic);
                    }
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

        Ok(interfaces)
    }

    async fn detach_interface(&self, resource: &Resource) -> Result<()> {
        match self
            .client
            .describe_network_interface_attribute(DescribeNetworkInterfaceAttributeRequest {
                network_interface_id: resource.id.to_string(),
                attribute: Some("attachment".to_string()),
                ..Default::default()
            })
            .await
        {
            Ok(result) => {
                if let Some(attachment) = result.attachment {
                    let req = self
                        .client
                        .detach_network_interface(DetachNetworkInterfaceRequest {
                            attachment_id: attachment.attachment_id.unwrap(),
                            force: Some(true),
                            ..Default::default()
                        });
                    handle_future!(req);
                }
            }
            Err(err) => {
                warn!(resource = resource.id.as_str(), error = ?err, "Failed getting attachment id.")
            }
        }

        Ok(())
    }

    async fn delete_eni(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting.");

        if !self.dry_run {
            self.detach_interface(resource).await?;

            let req = self
                .client
                .delete_network_interface(DeleteNetworkInterfaceRequest {
                    network_interface_id: resource.id.clone(),
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
impl NukerClient for Ec2EniClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized EC2 ENI resource scanner");
        let enis = self.get_enis().await?;
        Ok(self.package_resources(enis).await?)
    }

    async fn dependencies(&self, _resource: &Resource) -> Option<Vec<Resource>> {
        None
    }

    async fn additional_filters(
        &self,
        resource: &Resource,
        _config: &ResourceConfig,
    ) -> Option<bool> {
        if let Some(state) = resource.state {
            match state {
                ResourceState::Available => return Some(true),
                _ => return Some(false),
            }
        }

        Some(false)
    }

    async fn stop(&self, _resource: &Resource) -> Result<()> {
        Ok(())
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_eni(resource).await
    }
}
