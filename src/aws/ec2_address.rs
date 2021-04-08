use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_ec2::{Address, DescribeAddressesRequest, Ec2, Ec2Client, ReleaseAddressRequest, Tag};
use tracing::{debug, trace};

#[derive(Clone)]
pub struct Ec2AddressClient {
    client: Ec2Client,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl Ec2AddressClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        Ec2AddressClient {
            client: Ec2Client::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn package_resources(&self, mut addresses: Vec<Address>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for address in &mut addresses {
            let arn = format!(
                "arn:aws:ec2:{}:{}:eip/{}",
                self.region.name(),
                self.account_num,
                address.allocation_id.as_ref().unwrap(),
            );

            resources.push(Resource {
                id: address.allocation_id.take().unwrap(),
                arn: Some(arn),
                type_: ClientType::Ec2Address,
                region: self.region.clone(),
                tags: self.package_tags(address.tags.take()),
                state: if address.association_id.is_none() {
                    Some(ResourceState::Available)
                } else {
                    None
                },
                enforcement_reason: None,
                start_time: None,
                enforcement_state: EnforcementState::SkipUnknownState,
                resource_type: None,
                dependencies: None,
                termination_protection: None,
            });
        }

        Ok(resources)
    }

    async fn get_addresses(&self) -> Result<Vec<Address>> {
        let mut addresses: Vec<Address> = Vec::new();

        let req = self.client.describe_addresses(DescribeAddressesRequest {
            ..Default::default()
        });

        if let Ok(result) = handle_future_with_return!(req) {
            if result.addresses.is_some() {
                addresses.append(&mut result.addresses.unwrap())
            }
        }

        Ok(addresses)
    }

    async fn delete_address(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting.");

        if !self.dry_run {
            let req = self.client.release_address(ReleaseAddressRequest {
                allocation_id: Some(resource.id.to_owned()),
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
impl NukerClient for Ec2AddressClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized EC2 EIP resource scanner");
        let addresses = self.get_addresses().await?;
        Ok(self.package_resources(addresses).await?)
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
        self.delete_address(resource).await
    }
}
