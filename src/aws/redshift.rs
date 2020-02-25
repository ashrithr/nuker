use {
    crate::aws::cloudwatch::CwClient,
    crate::aws::Result,
    crate::config::RedshiftConfig,
    crate::service::{EnforcementState, NTag, NukeService, Resource, ResourceType},
    log::debug,
    rusoto_core::{HttpClient, Region},
    rusoto_credential::ProfileProvider,
    rusoto_redshift::{
        Cluster, DeleteClusterMessage, DescribeClustersMessage, Redshift, RedshiftClient, Tag,
    },
};

pub struct RedshiftNukeClient {
    pub client: RedshiftClient,
    pub cwclient: CwClient,
    pub config: RedshiftConfig,
    pub region: Region,
    pub dry_run: bool,
}

impl RedshiftNukeClient {
    pub fn new(
        profile_name: Option<&str>,
        region: Region,
        config: RedshiftConfig,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(RedshiftNukeClient {
                client: RedshiftClient::new_with(HttpClient::new()?, pp, region.clone()),
                cwclient: CwClient::new(profile_name, region.clone(), config.clone().idle_rules)?,
                config,
                region,
                dry_run,
            })
        } else {
            Ok(RedshiftNukeClient {
                client: RedshiftClient::new(region.clone()),
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

    fn package_clusters_as_resources(&self, clusters: Vec<Cluster>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for cluster in clusters {
            let cluster_id = cluster.cluster_identifier.as_ref().unwrap().to_owned();

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&cluster_id) {
                    EnforcementState::SkipConfig
                } else if cluster.cluster_status != Some("available".to_string()) {
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
                resource_type: ResourceType::Redshift,
                tags: self.package_tags_as_ntags(cluster.tags.clone()),
                state: cluster.cluster_status.clone(),
                enforcement_state,
            });
        }

        Ok(resources)
    }

    fn resource_tags_does_not_match(&self, cluster: &Cluster) -> bool {
        if !self.config.required_tags.is_empty() {
            !self.check_tags(&cluster.tags, &self.config.required_tags)
        } else {
            false
        }
    }

    fn resource_types_does_not_match(&self, cluster: &Cluster) -> bool {
        if !self.config.allowed_instance_types.is_empty() {
            !self
                .config
                .allowed_instance_types
                .contains(&cluster.node_type.clone().unwrap())
        } else {
            false
        }
    }

    fn is_resource_idle(&self, cluster: &Cluster) -> bool {
        if self.config.idle_rules.enabled {
            !self
                .cwclient
                .filter_rs_cluster_by_utilization(&cluster.cluster_identifier.as_ref().unwrap())
                .unwrap()
                && !self
                    .cwclient
                    .filter_rs_cluster_by_connections(&cluster.cluster_identifier.as_ref().unwrap())
                    .unwrap()
        } else {
            false
        }
    }

    fn get_clusters(&self) -> Result<Vec<Cluster>> {
        let mut next_token: Option<String> = None;
        let mut clusters: Vec<Cluster> = Vec::new();

        loop {
            let result = self
                .client
                .describe_clusters(DescribeClustersMessage {
                    marker: next_token,
                    ..Default::default()
                })
                .sync()?;

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
        }

        if !self.config.ignore.is_empty() {
            debug!("Ignoring Redshift Clusters: {:?}", self.config.ignore);
            clusters.retain(|c| {
                !self
                    .config
                    .ignore
                    .contains(&c.cluster_identifier.clone().unwrap())
            });
        }

        Ok(clusters)
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

    fn terminate_resource(&self, cluster_id: String) -> Result<()> {
        if !self.dry_run {
            self.client
                .delete_cluster(DeleteClusterMessage {
                    cluster_identifier: cluster_id,
                    ..Default::default()
                })
                .sync()?;
        }

        Ok(())
    }

    // Redshift does not have a Stop option, next closest option available is
    // to delete the cluster by taking a snapshot of the cluster and then restore
    // when needed.
    fn stop_resource(&self, cluster_id: String) -> Result<()> {
        if !self.dry_run {
            self.client
                .delete_cluster(DeleteClusterMessage {
                    cluster_identifier: cluster_id.clone(),
                    final_cluster_snapshot_identifier: Some(cluster_id),
                    final_cluster_snapshot_retention_period: Some(7), // retain for 7 days
                    ..Default::default()
                })
                .sync()?;
        }

        Ok(())
    }
}

impl NukeService for RedshiftNukeClient {
    fn scan(&self) -> Result<Vec<Resource>> {
        let clusters = self.get_clusters()?;

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
