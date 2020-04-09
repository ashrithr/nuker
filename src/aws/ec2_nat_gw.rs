use crate::aws::ClientDetails;
use crate::client::NukerClient;
use crate::config::ResourceConfig;
use crate::handle_future;
use crate::resource::Resource;
use crate::Result;
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_ec2::{DeleteNatGatewayRequest, Ec2, Ec2Client};
use tracing::{debug, trace};

#[derive(Clone)]
pub struct Ec2NatGWClient {
    client: Ec2Client,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl Ec2NatGWClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        Ec2NatGWClient {
            client: Ec2Client::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn delete_nat_gateway(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting");
        if !self.dry_run {
            let req = self.client.delete_nat_gateway(DeleteNatGatewayRequest {
                nat_gateway_id: resource.id.clone(),
            });
            handle_future!(req);
        }

        Ok(())
    }
}

#[async_trait]
impl NukerClient for Ec2NatGWClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized EC2 NAT Gateway resource scanner");

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
        self.delete_nat_gateway(resource).await
    }
}
