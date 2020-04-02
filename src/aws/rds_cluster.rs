use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_rds::{
    DBCluster, DBInstance, DeleteDBClusterMessage, DescribeDBClustersMessage,
    DescribeDBInstancesMessage, ListTagsForResourceMessage, ModifyDBClusterMessage, Rds, RdsClient,
    StopDBClusterMessage, Tag,
};
use std::str::FromStr;
use tracing::{debug, trace};

#[derive(Clone)]
pub struct RdsClusterClient {
    client: RdsClient,
    config: ResourceConfig,
    account_num: String,
    region: Region,
    dry_run: bool,
}

impl RdsClusterClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        RdsClusterClient {
            client: RdsClient::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
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

    async fn package_resources(&self, mut clusters: Vec<DBCluster>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for cluster in &mut clusters {
            let mut termination_protection: Option<bool> = None;

            if let Some(ref tp) = self.config.termination_protection {
                if tp.ignore {
                    termination_protection = cluster.deletion_protection;
                }
            }

            resources.push(Resource {
                id: cluster.db_cluster_identifier.take().unwrap(),
                arn: cluster.db_cluster_arn.take(),
                type_: ClientType::RdsCluster,
                region: self.region.clone(),
                resource_type: self.get_instance_types(&cluster).await.ok(),
                tags: self.package_tags(
                    self.list_tags(cluster.db_cluster_arn.as_ref().unwrap())
                        .await,
                ),
                state: Some(ResourceState::from_str(cluster.status.as_ref().unwrap()).unwrap()),
                start_time: cluster.cluster_create_time.take(),
                enforcement_state: EnforcementState::SkipUnknownState,
                dependencies: None,
                termination_protection,
            });
        }

        Ok(resources)
    }

    async fn get_clusters(&self) -> Result<Vec<DBCluster>> {
        let mut next_token: Option<String> = None;
        let mut clusters: Vec<DBCluster> = Vec::new();

        loop {
            let req = self.client.describe_db_clusters(DescribeDBClustersMessage {
                marker: next_token,
                ..Default::default()
            });

            if let Ok(result) = handle_future_with_return!(req) {
                if let Some(db_clusters) = result.db_clusters {
                    let mut temp_clusters: Vec<DBCluster> = db_clusters.into_iter().collect();

                    clusters.append(&mut temp_clusters);
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

    async fn get_dependencies(&self, resource: &Resource) -> Result<Vec<Resource>> {
        let mut dependencies = Vec::new();

        for db_instance in &mut self.get_cluster_members(resource.id.as_ref()).await? {
            dependencies.push(Resource {
                id: db_instance.db_instance_identifier.take().unwrap(),
                arn: db_instance.db_instance_arn.take(),
                type_: ClientType::RdsInstance,
                region: self.region.clone(),
                resource_type: db_instance.db_instance_class.to_owned().map(|t| vec![t]),
                tags: self.package_tags(
                    self.list_tags(db_instance.db_instance_arn.as_ref().unwrap())
                        .await,
                ),
                state: Some(
                    ResourceState::from_str(db_instance.db_instance_status.as_ref().unwrap())
                        .unwrap(),
                ),
                start_time: db_instance.instance_create_time.take(),
                enforcement_state: EnforcementState::SkipUnknownState,
                dependencies: None,
                termination_protection: None,
            });
        }

        Ok(dependencies)
    }

    async fn get_cluster_members(&self, cluster_id: &str) -> Result<Vec<DBInstance>> {
        let mut db_instances = Vec::new();

        let req = self.client.describe_db_clusters(DescribeDBClustersMessage {
            db_cluster_identifier: Some(cluster_id.to_owned()),
            ..Default::default()
        });

        if let Ok(mut resp) = handle_future_with_return!(req) {
            if let Some(mut db_cluster_members) = resp
                .db_clusters
                .take()
                .unwrap()
                .pop()
                .unwrap()
                .db_cluster_members
            {
                for member in &mut db_cluster_members {
                    let req = self
                        .client
                        .describe_db_instances(DescribeDBInstancesMessage {
                            db_instance_identifier: member.db_instance_identifier.take(),
                            ..Default::default()
                        });

                    if let Ok(result) = handle_future_with_return!(req) {
                        if let Some(instances) = result.db_instances {
                            for i in instances {
                                db_instances.push(i);
                            }
                        }
                    }
                }
            }
        }

        Ok(db_instances)
    }

    /// Fetch the instance types of each DBInstance which are part of the DBCluster
    async fn get_instance_types(&self, db_cluster_identifier: &DBCluster) -> Result<Vec<String>> {
        let mut instance_types: Vec<String> = Vec::new();

        if let Some(db_cluster_members) = &db_cluster_identifier.db_cluster_members {
            for db_member in db_cluster_members {
                let req = self
                    .client
                    .describe_db_instances(DescribeDBInstancesMessage {
                        db_instance_identifier: db_member.db_instance_identifier.clone(),
                        ..Default::default()
                    });

                if let Ok(result) = handle_future_with_return!(req) {
                    if let Some(instance) = result.db_instances {
                        instance_types.push(
                            instance
                                .first()
                                .unwrap()
                                .db_instance_class
                                .as_ref()
                                .unwrap()
                                .to_string(),
                        );
                    }
                }
            }
        }

        Ok(instance_types)
    }

    async fn list_tags(&self, arn: &String) -> Option<Vec<Tag>> {
        let req = self
            .client
            .list_tags_for_resource(ListTagsForResourceMessage {
                resource_name: arn.to_owned(),
                ..Default::default()
            });

        handle_future_with_return!(req)
            .ok()
            .map(|r| r.tag_list)
            .unwrap_or_default()
    }

    async fn disable_termination_protection(&self, cluster_id: &str) -> Result<()> {
        debug!(
            "Termination protection is enabled for: {}. Trying to disable it.",
            cluster_id
        );

        if !self.dry_run {
            let req = self.client.modify_db_cluster(ModifyDBClusterMessage {
                db_cluster_identifier: cluster_id.to_owned(),
                deletion_protection: Some(false),
                apply_immediately: Some(true),
                ..Default::default()
            });
            handle_future!(req);
        }

        Ok(())
    }

    async fn delete_cluster(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting");

        if !self.dry_run {
            if let Some(tp_enabled) = resource.termination_protection {
                if tp_enabled {
                    self.disable_termination_protection(resource.id.as_ref())
                        .await?;
                }
            }

            let req = self.client.delete_db_cluster(DeleteDBClusterMessage {
                db_cluster_identifier: resource.id.to_owned(),
                skip_final_snapshot: Some(true),
                ..Default::default()
            });

            handle_future!(req);
        }

        Ok(())
    }

    async fn stop_cluster(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Stopping");

        if !self.dry_run {
            let req = self.client.stop_db_cluster(StopDBClusterMessage {
                db_cluster_identifier: resource.id.to_owned(),
            });
            handle_future!(req);
        }

        Ok(())
    }
}

#[async_trait]
impl NukerClient for RdsClusterClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized RDS resource scanner");
        let clusters = self.get_clusters().await?;

        Ok(self.package_resources(clusters).await?)
    }

    async fn dependencies(&self, resource: &Resource) -> Option<Vec<Resource>> {
        self.get_dependencies(resource).await.ok()
    }

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.stop_cluster(resource).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_cluster(resource).await
    }

    fn additional_filters(&self, _resource: &Resource, _config: &ResourceConfig) -> Option<bool> {
        None
    }
}
