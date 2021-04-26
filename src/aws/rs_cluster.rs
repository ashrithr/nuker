use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_redshift::{
    Cluster, DeleteClusterMessage, DescribeClustersMessage, Redshift, RedshiftClient, Tag,
};
use std::str::FromStr;
use tracing::{debug, trace};

#[derive(Clone)]
pub struct RsClusterClient {
    client: RedshiftClient,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl RsClusterClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        RsClusterClient {
            client: RedshiftClient::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    fn package_tags(&self, tags: Option<Vec<Tag>>) -> Option<Vec<NTag>> {
        tags.map(|ts| {
            ts.iter()
                .map(|tag| NTag {
                    key: tag.key.clone(),
                    value: tag.value.clone(),
                })
                .collect()
        })
    }

    async fn package_resources(&self, clusters: Vec<Cluster>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for cluster in clusters {
            let cluster_id = cluster.cluster_identifier.as_ref().unwrap();
            let arn = format!(
                "arn:aws:redshift:{}:{}:cluster/{}",
                self.region.name(),
                self.account_num,
                cluster_id
            );

            resources.push(Resource {
                id: cluster.cluster_identifier.unwrap(),
                arn: Some(arn),
                type_: ClientType::RsCluster,
                region: self.region.clone(),
                tags: self.package_tags(cluster.tags),
                state: ResourceState::from_str(cluster.cluster_status.as_deref().unwrap()).ok(),
                start_time: cluster.cluster_create_time,
                resource_type: cluster.node_type.map(|t| vec![t]),
                enforcement_state: EnforcementState::SkipUnknownState,
                enforcement_reason: None,
                dependencies: None,
                termination_protection: None,
            });
        }

        Ok(resources)
    }

    async fn get_clusters(&self) -> Result<Vec<Cluster>> {
        let mut next_token: Option<String> = None;
        let mut clusters: Vec<Cluster> = Vec::new();

        loop {
            let req = self.client.describe_clusters(DescribeClustersMessage {
                marker: next_token,
                ..Default::default()
            });

            if let Ok(result) = handle_future_with_return!(req) {
                if let Some(cls) = result.clusters {
                    for c in cls {
                        clusters.push(c);
                    }
                }

                if result.marker.is_none() {
                    break;
                } else {
                    next_token = result.marker;
                }
            } else {
                break;
            }
        }

        Ok(clusters)
    }

    async fn terminate_resource(&self, cluster_id: String) -> Result<()> {
        debug!(resource = cluster_id.as_str(), "Deleting");

        if !self.dry_run {
            let req = self.client.delete_cluster(DeleteClusterMessage {
                cluster_identifier: cluster_id,
                ..Default::default()
            });

            handle_future!(req);
        }

        Ok(())
    }

    // Redshift does not have a Stop option, next closest option available is
    // to delete the cluster by taking a snapshot of the cluster and then restore
    // when needed.
    async fn stop_resource(&self, cluster_id: String) -> Result<()> {
        debug!(resource = cluster_id.as_str(), "Deleting");

        if !self.dry_run {
            let req = self.client.delete_cluster(DeleteClusterMessage {
                cluster_identifier: cluster_id.clone(),
                final_cluster_snapshot_identifier: Some(cluster_id),
                final_cluster_snapshot_retention_period: Some(7), // retain for 7 days
                ..Default::default()
            });

            handle_future!(req);
        }

        Ok(())
    }
}

#[async_trait]
impl NukerClient for RsClusterClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized Redshift resource scanner");
        let clusters = self.get_clusters().await?;

        Ok(self.package_resources(clusters).await?)
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
        self.stop_resource(resource.id.to_owned()).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.terminate_resource(resource.id.to_owned()).await
    }
}
