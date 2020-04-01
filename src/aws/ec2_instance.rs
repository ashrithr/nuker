use crate::client::{NukerClient, ResourceCleaner, ResourceScanner};
use crate::config::Ec2Config;
use crate::resource::Resource;
use crate::Event;
use crate::NSender;
use async_trait::async_trait;
use rusoto_core::{Client, Region};
use rusoto_ec2::{Ec2, Ec2Client};

#[derive(Clone)]
pub struct Ec2Instance {
    client: Ec2Client,
    config: Ec2Config,
}

impl Ec2Instance {
    pub fn new(client: &Client, region: &Region, config: Ec2Config) -> Self {
        Ec2Instance {
            client: Ec2Client::new_with_client(client.clone(), region.clone()),
            config,
        }
    }
}

#[async_trait]
impl ResourceScanner for Ec2Instance {
    async fn scan(&self) -> Vec<Resource> {
        unimplemented!()
    }

    async fn dependencies(&self, resource: &Resource) -> Option<Vec<Resource>> {
        unimplemented!()
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

impl NukerClient for Ec2Instance {}
