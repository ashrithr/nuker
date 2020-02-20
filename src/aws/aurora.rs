use {
    crate::aws::cloudwatch::CwClient,
    crate::aws::Result,
    crate::config::AuroraConfig,
    crate::config::TargetState,
    crate::service::{NTag, NukeService, Resource, ResourceType},
    log::{debug, info, trace},
    rusoto_core::{HttpClient, Region},
    rusoto_credential::ProfileProvider,
    rusoto_rds::{
        DBCluster, DeleteDBClusterMessage, DescribeDBClustersMessage, DescribeDBInstancesMessage,
        Filter, ListTagsForResourceMessage, ModifyDBClusterMessage, Rds, RdsClient,
        StopDBClusterMessage, Tag,
    },
};

pub struct AuroraNukeClient {
    pub client: RdsClient,
    pub cwclient: CwClient,
    pub config: AuroraConfig,
    pub dry_run: bool,
}

impl AuroraNukeClient {
    pub fn new(
        profile_name: Option<&str>,
        region: Region,
        config: AuroraConfig,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(AuroraNukeClient {
                client: RdsClient::new_with(HttpClient::new()?, pp, region.clone()),
                cwclient: CwClient::new(profile_name, region, config.clone().idle_rules)?,
                config: config,
                dry_run: dry_run,
            })
        } else {
            Ok(AuroraNukeClient {
                client: RdsClient::new(region.clone()),
                cwclient: CwClient::new(profile_name, region, config.clone().idle_rules)?,
                config: config,
                dry_run: dry_run,
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

    fn package_clusters_as_resources(&self, clusters: Vec<&DBCluster>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for cluster in clusters {
            let cluster_id = cluster.db_cluster_identifier.as_ref().unwrap().to_owned();

            if let Some(_resource) = resources.iter().find(|r| r.id == cluster_id) {
                trace!("Skipping resource, already exists in the list.");
            } else {
                resources.push(Resource {
                    id: cluster_id,
                    resource_type: ResourceType::Aurora,
                    tags: self
                        .package_tags_as_ntags(self.list_tags(cluster.db_cluster_arn.clone())?),
                    state: cluster.status.clone(),
                });
            }
        }

        Ok(resources)
    }

    fn get_clusters(&self, filter: Vec<Filter>) -> Result<Vec<DBCluster>> {
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
                .sync()?;

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

    fn list_tags(&self, arn: Option<String>) -> Result<Option<Vec<Tag>>> {
        let result = self
            .client
            .list_tags_for_resource(ListTagsForResourceMessage {
                resource_name: arn.unwrap(),
                ..Default::default()
            })
            .sync()?;
        Ok(result.tag_list)
    }

    fn filter_by_tags<'a>(&self, clusters: &Vec<&'a DBCluster>) -> Vec<&'a DBCluster> {
        debug!(
            "Total # of db clusters before applying Filter by required tags - {:?}: {}.",
            &self.config.required_tags,
            clusters.len()
        );

        clusters
            .iter()
            .filter(|cluster| {
                !self.check_tags(
                    &self
                        .list_tags(cluster.db_cluster_arn.clone())
                        .unwrap_or_default(),
                    &self.config.required_tags,
                )
            })
            .cloned()
            .collect()
    }

    fn check_tags(&self, tags: &Option<Vec<Tag>>, required_tags: &Vec<String>) -> bool {
        let tags: Vec<String> = tags
            .clone()
            .unwrap_or_default()
            .iter()
            .map(|t| t.key.clone().unwrap())
            .collect();
        required_tags.iter().all(|rt| tags.contains(rt))
    }

    fn filter_by_types<'a>(&self, clusters: &Vec<&'a DBCluster>) -> Vec<&'a DBCluster> {
        let mut filtered_clusters: Vec<&DBCluster> = Vec::new();

        debug!(
            "Total # of clusters before applying Filter by Instance type - {:?}: {}",
            self.config.allowed_instance_types,
            clusters.len()
        );

        for cluster in clusters {
            if let Ok(instance_types) = self.get_instance_types(cluster) {
                for instance in instance_types {
                    if self
                        .config
                        .allowed_instance_types
                        .iter()
                        .any(|it| Some(it) == Some(&instance))
                    {
                        // Insert only if the element is not already present
                        if filtered_clusters
                            .iter()
                            .find(|c| c.db_cluster_identifier == cluster.db_cluster_identifier)
                            .is_none()
                        {
                            filtered_clusters.push(cluster)
                        }
                    }
                }
            }
        }

        filtered_clusters
    }

    /// Fetch the instance types of each DBInstance which are part of the DBCluster
    fn get_instance_types(&self, db_cluster_identifier: &DBCluster) -> Result<Vec<String>> {
        let mut instance_types: Vec<String> = Vec::new();

        if let Some(db_cluster_members) = &db_cluster_identifier.db_cluster_members {
            for db_member in db_cluster_members {
                let result = self
                    .client
                    .describe_db_instances(DescribeDBInstancesMessage {
                        db_instance_identifier: db_member.db_instance_identifier.clone(),
                        ..Default::default()
                    })
                    .sync()?;

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

    fn filter_by_idle_rules<'a>(&self, clusters: &Vec<&'a DBCluster>) -> Vec<&'a DBCluster> {
        debug!(
            "Total # of clusters before applying Filter by CPU Utilization - {:?}: {}",
            self.config.idle_rules,
            clusters.len()
        );

        clusters
            .iter()
            .filter(|cluster| {
                cluster.status == Some("available".to_string())
                    && !self
                        .cwclient
                        .filter_db_cluster_by_utilization(
                            &cluster.db_cluster_identifier.as_ref().unwrap(),
                        )
                        .unwrap()
                    && !self
                        .cwclient
                        .filter_db_cluster_by_connections(
                            &cluster.db_cluster_identifier.as_ref().unwrap(),
                        )
                        .unwrap()
            })
            .cloned()
            .collect()
    }

    fn disable_termination_protection(&self, cluster_id: &str) -> Result<()> {
        let resp = self
            .client
            .describe_db_clusters(DescribeDBClustersMessage {
                db_cluster_identifier: Some(cluster_id.to_owned()),
                ..Default::default()
            })
            .sync()?;

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
                        .sync()?;
                }
            }
        }

        Ok(())
    }

    fn stop_clusters(&self, cluster_ids: &Vec<String>) -> Result<()> {
        debug!("Stopping clusters: {:?}", cluster_ids);

        if !self.dry_run {
            for cluster_id in cluster_ids {
                self.client
                    .stop_db_cluster(StopDBClusterMessage {
                        db_cluster_identifier: cluster_id.to_owned(),
                    })
                    .sync()?;
            }
        }

        Ok(())
    }

    fn terminate_clusters(&self, cluster_ids: &Vec<String>) -> Result<()> {
        debug!("Terminating instances: {:?}", cluster_ids);

        if self.config.termination_protection.ignore {
            for cluster_id in cluster_ids {
                self.disable_termination_protection(cluster_id)?;
            }
        }

        if !self.dry_run {
            for cluster_id in cluster_ids {
                self.client
                    .delete_db_cluster(DeleteDBClusterMessage {
                        db_cluster_identifier: cluster_id.to_owned(),
                        ..Default::default()
                    })
                    .sync()?;
            }
        }

        Ok(())
    }
}

impl NukeService for AuroraNukeClient {
    fn scan(&self) -> Result<Vec<Resource>> {
        let mut filtered_clusters: Vec<&DBCluster> = Vec::new();
        let clusters: Vec<DBCluster> = self.get_clusters(Vec::new())?;

        let running_clusters: Vec<&DBCluster> = clusters
            .iter()
            .filter(|c| c.status == Some("available".to_string()))
            .collect();
        let stopped_clusters: Vec<&DBCluster> = clusters
            .iter()
            .filter(|c| c.status == Some("stopped".to_string()))
            .collect();
        let mut clusters_filtered_by_tags = self.filter_by_tags(&running_clusters);
        let mut clusters_filtered_by_types = self.filter_by_types(&running_clusters);
        let mut idle_clusters = self.filter_by_idle_rules(&running_clusters);

        info!(
            "Aurora Summary: \n\
             \tTotal Aurora Clusters: {} \n\
             \tRunning Aurora Clusters: {} \n\
             \tStopped Aurora Clusters: {} \n\
             \tAurora Clusters that do not have required tags: {} \n\
             \tAurora Clusters that are not using the allowed instance-types: {} \n\
             \tAurora Clusters that are idle: {}",
            clusters.len(),
            running_clusters.len(),
            stopped_clusters.len(),
            clusters_filtered_by_tags.len(),
            clusters_filtered_by_types.len(),
            idle_clusters.len()
        );

        filtered_clusters.append(&mut clusters_filtered_by_tags);
        filtered_clusters.append(&mut clusters_filtered_by_types);
        filtered_clusters.append(&mut idle_clusters);

        Ok(self.package_clusters_as_resources(filtered_clusters)?)
    }

    fn cleanup(&self, resources: Vec<&Resource>) -> Result<()> {
        let cluster_ids = resources
            .into_iter()
            .map(|r| r.id.clone())
            .collect::<Vec<String>>();

        match self.config.target_state {
            TargetState::Stopped => Ok(self.stop_clusters(&cluster_ids)?),
            TargetState::Terminated | TargetState::Deleted => {
                Ok(self.terminate_clusters(&cluster_ids)?)
            }
        }
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
