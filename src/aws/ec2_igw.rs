use crate::aws::ClientDetails;
use crate::client::NukerClient;
use crate::config::ResourceConfig;
use crate::resource::Resource;
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_ec2::{
    DeleteInternetGatewayRequest, DescribeInternetGatewaysRequest, DetachInternetGatewayRequest,
    Ec2, Ec2Client,
};
use tracing::{debug, trace};

#[derive(Clone)]
pub struct Ec2IgwClient {
    client: Ec2Client,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl Ec2IgwClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        Ec2IgwClient {
            client: Ec2Client::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn delete_igw(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Detaching & Deleting");
        if !self.dry_run {
            if let Ok(result) = handle_future_with_return!(self.client.describe_internet_gateways(
                DescribeInternetGatewaysRequest {
                    internet_gateway_ids: Some(vec![resource.id.clone()]),
                    ..Default::default()
                }
            )) {
                if let Some(internet_gateways) = result.internet_gateways {
                    for igw in internet_gateways {
                        if let Some(attachments) = igw.attachments {
                            for attachment in attachments {
                                let vpc_id = attachment.vpc_id.unwrap();

                                let req = self.client.detach_internet_gateway(
                                    DetachInternetGatewayRequest {
                                        internet_gateway_id: resource.id.clone(),
                                        vpc_id,
                                        ..Default::default()
                                    },
                                );
                                handle_future!(req);

                                let req = self.client.delete_internet_gateway(
                                    DeleteInternetGatewayRequest {
                                        internet_gateway_id: resource.id.clone(),
                                        ..Default::default()
                                    },
                                );
                                handle_future!(req);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl NukerClient for Ec2IgwClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized EC2 IGW resource scanner");

        Ok(vec![])
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
        self.delete_igw(resource).await
    }
}
