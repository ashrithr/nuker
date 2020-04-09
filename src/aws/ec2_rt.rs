use crate::aws::ClientDetails;
use crate::client::NukerClient;
use crate::config::ResourceConfig;
use crate::handle_future;
use crate::resource::Resource;
use crate::Result;
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_ec2::{DeleteRouteTableRequest, Ec2, Ec2Client};
use tracing::{debug, trace};

#[derive(Clone)]
pub struct Ec2RtClient {
    client: Ec2Client,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl Ec2RtClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        Ec2RtClient {
            client: Ec2Client::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn delete_rt(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting");
        if !self.dry_run {
            let req = self.client.delete_route_table(DeleteRouteTableRequest {
                route_table_id: resource.id.clone(),
                ..Default::default()
            });
            handle_future!(req);
        }

        Ok(())
    }
}

#[async_trait]
impl NukerClient for Ec2RtClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized EC2 RouteTable resource scanner");

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
        self.delete_rt(resource).await
    }
}
