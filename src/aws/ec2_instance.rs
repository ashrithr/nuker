use crate::aws::CwClient;
use crate::client::{NukerClient, ResourceCleaner, ResourceFilter, ResourceScanner};
use crate::config::ResourceConfig;
use crate::resource::Resource;
use crate::Event;
use crate::NSender;
use async_trait::async_trait;
use rusoto_core::{Client, Region};
use rusoto_ec2::{Ec2, Ec2Client};
use std::sync::Arc;
use tracing::{debug, trace, warn};

const RUNNING_STATE: i64 = 16;
const STOPPED_STATE: i64 = 80;

#[derive(Clone)]
pub struct Ec2Instance {
    client: Ec2Client,
    cw_client: Arc<Box<CwClient>>,
    config: ResourceConfig,
    dry_run: bool,
}

impl Ec2Instance {
    pub fn new(
        client: &Client,
        region: &Region,
        config: ResourceConfig,
        cw_client: Arc<Box<CwClient>>,
        dry_run: bool,
    ) -> Self {
        Ec2Instance {
            client: Ec2Client::new_with_client(client.clone(), region.clone()),
            cw_client,
            config,
            dry_run,
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

#[async_trait]
impl ResourceFilter for Ec2Instance {
    fn additional_filters(&self, resource: &Resource, config: &ResourceConfig) -> bool {
        false
    }
}

impl NukerClient for Ec2Instance {}
