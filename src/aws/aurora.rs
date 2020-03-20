use crate::{
    aws::{cloudwatch::CwClient, util, Result},
    config::{AuroraConfig, RequiredTags},
    resource::{EnforcementState, NTag, Resource, ResourceType},
    service::NukerService,
};
use async_trait::async_trait;
use chrono::Utc;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use rusoto_rds::{
    DBCluster, DeleteDBClusterMessage, DeleteDBInstanceMessage, DescribeDBClustersMessage,
    DescribeDBInstancesMessage, DescribeEventsMessage, Filter, ListTagsForResourceMessage,
    ModifyDBClusterMessage, Rds, RdsClient, StopDBClusterMessage, Tag,
};
use std::sync::Arc;
use tracing::{debug, trace};

const DB_STATUS_AVAILABLE: &str = "available";
const DB_STATUS_STOPPED: &str = "stopped";

#[derive(Clone)]
pub struct AuroraService {
    pub client: RdsClient,
    pub cw_client: Arc<Box<CwClient>>,
    pub config: AuroraConfig,
    pub region: Region,
    pub dry_run: bool,
}

impl AuroraService {
    pub fn new(
        profile_name: Option<String>,
        region: Region,
        config: AuroraConfig,
        cw_client: Arc<Box<CwClient>>,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = &profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(AuroraService {
                client: RdsClient::new_with(HttpClient::new()?, pp, region.clone()),
                cw_client,
                config,
                region,
                dry_run,
            })
        } else {
            Ok(AuroraService {
                client: RdsClient::new(region.clone()),
                cw_client,
                config,
                region,
                dry_run,
            })
        }
    }

    fn package_tags_as_ntags(&self, tags: Option<Vec<Tag>>) -> Option<Vec<NTag>> {
        tags.map(|ts| {
            ts.iter()
                .map(|tag| NTag {
                    key: tag.key.clone(),
                    value: tag.value.clone(),
                })
                .collect()
        })
    }

    async fn package_clusters_as_resources(
        &self,
        clusters: Vec<DBCluster>,
    ) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for cluster in clusters {
            let cluster_id = cluster.db_cluster_identifier.as_ref().unwrap().to_owned();

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&cluster_id) {
                    EnforcementState::SkipConfig
                } else if self.is_resource_stopped_older(&cluster).await {
                    debug!(
                        resource = cluster_id.as_str(),
                        "Cluster is stopped for longer than {:?}",
                        self.config.manage_stopped.older_than
                    );
                    EnforcementState::Delete
                } else if cluster.status != Some(DB_STATUS_AVAILABLE.to_string()) {
                    EnforcementState::SkipStopped
                } else {
                    if self.resource_tags_does_not_match(&cluster).await {
                        debug!(
                            resource = cluster_id.as_str(),
                            "Cluster tags does not match"
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.resource_types_does_not_match(&cluster).await {
                        debug!(
                            resource = cluster_id.as_str(),
                            "Cluster types does not match"
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.is_resource_idle(&cluster).await {
                        debug!(resource = cluster_id.as_str(), "Cluster is idle");
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else {
                        EnforcementState::Skip
                    }
                }
            };

            resources.push(Resource {
                id: cluster_id,
                arn: cluster.db_cluster_arn.clone(),
                region: self.region.clone(),
                resource_type: ResourceType::Aurora,
                tags: self
                    .package_tags_as_ntags(self.list_tags(cluster.db_cluster_arn.clone()).await?),
                state: cluster.status.clone(),
                enforcement_state,
            });
        }

        Ok(resources)
    }

    async fn resource_tags_does_not_match(&self, cluster: &DBCluster) -> bool {
        if self.config.required_tags.is_some() {
            !self.check_tags(
                &self
                    .list_tags(cluster.db_cluster_arn.clone())
                    .await
                    .unwrap_or_default(),
                &self.config.required_tags.as_ref().unwrap(),
            )
        } else {
            false
        }
    }

    async fn is_resource_stopped_older(&self, cluster: &DBCluster) -> bool {
        if self.config.manage_stopped.enabled
            && cluster.status == Some(DB_STATUS_STOPPED.to_string())
        {
            match self
                .client
                .describe_events(DescribeEventsMessage {
                    duration: Some(20160), // 14 days - Max
                    event_categories: Some(vec!["notification".to_string()]),
                    source_identifier: cluster.db_cluster_identifier.clone(),
                    source_type: Some("db-cluster".to_string()),
                    ..Default::default()
                })
                .await
            {
                Ok(event_message) => {
                    if let Some(events) = event_message.events {
                        for event in events {
                            if event.message == Some("DB cluster stopped".to_string()) {
                                let stopped_date =
                                    event.date.unwrap_or(format!("{:?}", Utc::now()));

                                if util::is_ts_older_than(
                                    stopped_date.as_str(),
                                    &self.config.manage_stopped.older_than,
                                ) {
                                    return true;
                                } else {
                                    trace!(
                                        resource = cluster
                                            .db_cluster_identifier
                                            .as_ref()
                                            .unwrap()
                                            .as_str(),
                                        "DB Cluster is stopped at {:?} and is not old enough {:?}",
                                        stopped_date,
                                        self.config.manage_stopped.older_than
                                    );
                                }
                            }
                        }
                    }
                }
                Err(err) => trace!(
                    resource = cluster.db_cluster_identifier.as_ref().unwrap().as_str(),
                    "Failed getting describe events: {:?}",
                    err
                ),
            }
        }

        false
    }

    async fn resource_types_does_not_match(&self, cluster: &DBCluster) -> bool {
        if !self.config.allowed_instance_types.is_empty() {
            if let Ok(instance_types) = self.get_instance_types(cluster).await {
                if instance_types
                    .iter()
                    .any(|it| !self.config.allowed_instance_types.contains(&it))
                {
                    return true;
                }
            }
            false
        } else {
            false
        }
    }

    async fn is_resource_idle(&self, cluster: &DBCluster) -> bool {
        if self.config.idle_rules.is_some() {
            !self
                .cw_client
                .filter_db_cluster(&cluster.db_cluster_identifier.as_ref().unwrap())
                .await
                .unwrap()
        } else {
            false
        }
    }

    async fn get_clusters(&self, filter: Vec<Filter>) -> Result<Vec<DBCluster>> {
        let mut next_token: Option<String> = None;
        let mut clusters: Vec<DBCluster> = Vec::new();

        loop {
            let result = self
                .client
                .describe_db_clusters(DescribeDBClustersMessage {
                    filters: Some(filter.clone()),
                    marker: next_token,
                    ..Default::default()
                })
                .await?;

            if let Some(db_clusters) = result.db_clusters {
                let mut temp_clusters: Vec<DBCluster> = db_clusters.into_iter().collect();

                clusters.append(&mut temp_clusters);
            }

            if result.marker.is_none() {
                break;
            } else {
                next_token = result.marker;
            }
        }

        if !self.config.ignore.is_empty() {
            debug!("Ignoring the DB clusters: {:?}", self.config.ignore);
            clusters.retain(|c| {
                !self
                    .config
                    .ignore
                    .contains(&c.db_cluster_identifier.clone().unwrap())
            });
        }

        Ok(clusters)
    }

    async fn list_tags(&self, arn: Option<String>) -> Result<Option<Vec<Tag>>> {
        let result = self
            .client
            .list_tags_for_resource(ListTagsForResourceMessage {
                resource_name: arn.unwrap(),
                ..Default::default()
            })
            .await?;
        Ok(result.tag_list)
    }

    fn check_tags(&self, tags: &Option<Vec<Tag>>, required_tags: &Vec<RequiredTags>) -> bool {
        let ntags = self.package_tags_as_ntags(tags.to_owned());
        util::compare_tags(ntags, required_tags)
    }

    /// Fetch the instance types of each DBInstance which are part of the DBCluster
    async fn get_instance_types(&self, db_cluster_identifier: &DBCluster) -> Result<Vec<String>> {
        let mut instance_types: Vec<String> = Vec::new();

        if let Some(db_cluster_members) = &db_cluster_identifier.db_cluster_members {
            for db_member in db_cluster_members {
                let result = self
                    .client
                    .describe_db_instances(DescribeDBInstancesMessage {
                        db_instance_identifier: db_member.db_instance_identifier.clone(),
                        ..Default::default()
                    })
                    .await?;

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

        Ok(instance_types)
    }

    async fn disable_termination_protection(&self, cluster_id: &str) -> Result<()> {
        let resp = self
            .client
            .describe_db_clusters(DescribeDBClustersMessage {
                db_cluster_identifier: Some(cluster_id.to_owned()),
                ..Default::default()
            })
            .await?;

        if resp.db_clusters.is_some() {
            if resp
                .db_clusters
                .unwrap()
                .first()
                .unwrap()
                .deletion_protection
                == Some(true)
            {
                debug!(
                    "Termination protection is enabled for: {}. Trying to disable it.",
                    cluster_id
                );

                if !self.dry_run {
                    self.client
                        .modify_db_cluster(ModifyDBClusterMessage {
                            db_cluster_identifier: cluster_id.to_owned(),
                            deletion_protection: Some(false),
                            apply_immediately: Some(true),
                            ..Default::default()
                        })
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn stop_resource(&self, cluster_id: String) -> Result<()> {
        debug!(resource = cluster_id.as_str(), "Stopping");

        if !self.dry_run {
            self.client
                .stop_db_cluster(StopDBClusterMessage {
                    db_cluster_identifier: cluster_id,
                })
                .await?;
        }

        Ok(())
    }

    async fn terminate_db_instance(&self, instance_id: Option<String>) -> Result<()> {
        debug!(
            resource = instance_id.as_ref().unwrap().as_str(),
            "Deleting"
        );

        if !self.dry_run && instance_id.is_some() {
            self.client
                .delete_db_instance(DeleteDBInstanceMessage {
                    db_instance_identifier: instance_id.unwrap(),
                    skip_final_snapshot: Some(true),
                    ..Default::default()
                })
                .await?;
        }

        Ok(())
    }

    async fn terminate_resource(&self, cluster_id: String) -> Result<()> {
        debug!(resource = cluster_id.as_str(), "Deleting");

        if !self.dry_run {
            if self.config.termination_protection.ignore {
                self.disable_termination_protection(&cluster_id).await?;
            }

            // Delete all cluster members
            if let Some(db_clusters) = self
                .client
                .describe_db_clusters(DescribeDBClustersMessage {
                    db_cluster_identifier: Some(cluster_id.clone()),
                    ..Default::default()
                })
                .await?
                .db_clusters
            {
                for db_cluster in db_clusters {
                    if let Some(cluster_members) = db_cluster.db_cluster_members {
                        for member in cluster_members {
                            self.terminate_db_instance(member.db_instance_identifier)
                                .await?
                        }
                    }
                }
            }

            self.client
                .delete_db_cluster(DeleteDBClusterMessage {
                    db_cluster_identifier: cluster_id,
                    skip_final_snapshot: Some(true),
                    ..Default::default()
                })
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl NukerService for AuroraService {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized Aurora resource scanner");
        let clusters = self.get_clusters(Vec::new()).await?;

        Ok(self.package_clusters_as_resources(clusters).await?)
    }

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.stop_resource(resource.id.to_owned()).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.terminate_resource(resource.id.to_owned()).await
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
