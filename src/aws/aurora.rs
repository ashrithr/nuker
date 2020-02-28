use {
    crate::aws::cloudwatch::CwClient,
    crate::aws::util,
    crate::aws::Result,
    crate::config::{AuroraConfig, RequiredTags},
    crate::service::{EnforcementState, NTag, NukeService, Resource, ResourceType},
    log::debug,
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
    pub region: Region,
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
                cwclient: CwClient::new(profile_name, region.clone(), config.clone().idle_rules)?,
                config,
                region,
                dry_run,
            })
        } else {
            Ok(AuroraNukeClient {
                client: RdsClient::new(region.clone()),
                cwclient: CwClient::new(profile_name, region.clone(), config.clone().idle_rules)?,
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

    fn package_clusters_as_resources(&self, clusters: Vec<DBCluster>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for cluster in clusters {
            let cluster_id = cluster.db_cluster_identifier.as_ref().unwrap().to_owned();

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&cluster_id) {
                    EnforcementState::SkipConfig
                } else if cluster.status != Some("available".to_string()) {
                    EnforcementState::SkipStopped
                } else {
                    if self.resource_tags_does_not_match(&cluster) {
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.resource_types_does_not_match(&cluster) {
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.is_resource_idle(&cluster) {
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else {
                        EnforcementState::Skip
                    }
                }
            };

            resources.push(Resource {
                id: cluster_id,
                region: self.region.clone(),
                resource_type: ResourceType::Aurora,
                tags: self.package_tags_as_ntags(self.list_tags(cluster.db_cluster_arn.clone())?),
                state: cluster.status.clone(),
                enforcement_state,
            });
        }

        Ok(resources)
    }

    fn resource_tags_does_not_match(&self, cluster: &DBCluster) -> bool {
        if !self.config.required_tags.is_empty() {
            !self.check_tags(
                &self
                    .list_tags(cluster.db_cluster_arn.clone())
                    .unwrap_or_default(),
                &self.config.required_tags,
            )
        } else {
            false
        }
    }

    fn resource_types_does_not_match(&self, cluster: &DBCluster) -> bool {
        if !self.config.allowed_instance_types.is_empty() {
            if let Ok(instance_types) = self.get_instance_types(cluster) {
                if instance_types
                    .iter()
                    .any(|it| self.config.allowed_instance_types.contains(&it))
                {
                    return true;
                }
            }
            false
        } else {
            false
        }
    }

    fn is_resource_idle(&self, cluster: &DBCluster) -> bool {
        if !self.config.idle_rules.is_empty() {
            !self
                .cwclient
                .filter_db_cluster(&cluster.db_cluster_identifier.as_ref().unwrap())
                .unwrap()
        } else {
            false
        }
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

    fn check_tags(&self, tags: &Option<Vec<Tag>>, required_tags: &Vec<RequiredTags>) -> bool {
        let ntags = self.package_tags_as_ntags(tags.to_owned());
        util::compare_tags(ntags, required_tags)
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

    fn stop_resource(&self, cluster_id: String) -> Result<()> {
        debug!("Stopping cluster: {:?}", cluster_id);

        if !self.dry_run {
            self.client
                .stop_db_cluster(StopDBClusterMessage {
                    db_cluster_identifier: cluster_id,
                })
                .sync()?;
        }

        Ok(())
    }

    fn terminate_resource(&self, cluster_id: String) -> Result<()> {
        debug!("Terminating instances: {:?}", cluster_id);

        if !self.dry_run {
            if self.config.termination_protection.ignore {
                self.disable_termination_protection(&cluster_id)?;
            }

            self.client
                .delete_db_cluster(DeleteDBClusterMessage {
                    db_cluster_identifier: cluster_id,
                    ..Default::default()
                })
                .sync()?;
        }

        Ok(())
    }
}

impl NukeService for AuroraNukeClient {
    fn scan(&self) -> Result<Vec<Resource>> {
        let clusters = self.get_clusters(Vec::new())?;

        Ok(self.package_clusters_as_resources(clusters)?)
    }

    fn stop(&self, resource: &Resource) -> Result<()> {
        self.stop_resource(resource.id.to_owned())
    }

    fn delete(&self, resource: &Resource) -> Result<()> {
        self.terminate_resource(resource.id.to_owned())
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
