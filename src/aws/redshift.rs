use {
    crate::aws::cloudwatch::CwClient,
    crate::aws::Result,
    crate::config::{RedshiftConfig, TargetState},
    crate::service::{NTag, NukeService, Resource, ResourceType},
    log::{debug, info, trace},
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
                cwclient: CwClient::new(profile_name, region, config.clone().idle_rules)?,
                config,
                dry_run,
            })
        } else {
            Ok(RedshiftNukeClient {
                client: RedshiftClient::new(region.clone()),
                cwclient: CwClient::new(profile_name, region, config.clone().idle_rules)?,
                config,
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

    fn package_clusters_as_resources(&self, clusters: Vec<&Cluster>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for cluster in clusters {
            let cluster_id = cluster.cluster_identifier.as_ref().unwrap().to_owned();

            if let Some(_resource) = resources.iter().find(|r| r.id == cluster_id) {
                trace!("Skipping resource, already exists in the list");
            } else {
                resources.push(Resource {
                    id: cluster_id,
                    resource_type: ResourceType::Redshift,
                    tags: self.package_tags_as_ntags(cluster.tags.clone()),
                    state: cluster.cluster_status.clone(),
                });
            }
        }

        Ok(resources)
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

    fn filter_by_tags<'a>(&self, clusters: &Vec<&'a Cluster>) -> Vec<&'a Cluster> {
        clusters
            .iter()
            .filter(|c| !self.check_tags(&c.tags, &self.config.required_tags))
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

    fn filter_by_idle_rules<'a>(&self, clusters: &Vec<&'a Cluster>) -> Vec<&'a Cluster> {
        clusters
            .iter()
            .filter(|c| {
                c.cluster_status == Some("available".to_string())
                    && !self
                        .cwclient
                        .filter_rs_cluster_by_utilization(&c.cluster_identifier.as_ref().unwrap())
                        .unwrap()
                    && !self
                        .cwclient
                        .filter_rs_cluster_by_connections(&c.cluster_identifier.as_ref().unwrap())
                        .unwrap()
            })
            .cloned()
            .collect()
    }

    fn filter_by_types<'a>(&self, clusters: &Vec<&'a Cluster>) -> Vec<&'a Cluster> {
        clusters
            .iter()
            .filter(|c| {
                self.config
                    .allowed_instance_types
                    .contains(&c.node_type.clone().unwrap())
            })
            .cloned()
            .collect()
    }

    fn terminate_clusters(&self, cluster_ids: &Vec<String>) -> Result<()> {
        if !self.dry_run {
            for cluster_id in cluster_ids {
                self.client
                    .delete_cluster(DeleteClusterMessage {
                        cluster_identifier: cluster_id.to_string(),
                        ..Default::default()
                    })
                    .sync()?;
            }
        }

        Ok(())
    }

    // Redshift does not have a Stop option, next closest option available is
    // to delete the cluster by taking a snapshot of the cluster and then restore
    // when needed.
    fn stop_clusters(&self, cluster_ids: &Vec<String>) -> Result<()> {
        if !self.dry_run {
            for cluster_id in cluster_ids {
                self.client
                    .delete_cluster(DeleteClusterMessage {
                        cluster_identifier: cluster_id.to_string(),
                        final_cluster_snapshot_identifier: Some(cluster_id.to_string()),
                        final_cluster_snapshot_retention_period: Some(7), // retain for 7 days
                        ..Default::default()
                    })
                    .sync()?;
            }
        }

        Ok(())
    }
}

impl NukeService for RedshiftNukeClient {
    fn scan(&self) -> Result<Vec<Resource>> {
        let mut filtered_clusters: Vec<&Cluster> = Vec::new();
        let clusters: Vec<Cluster> = self.get_clusters()?;

        let running_clusters = clusters
            .iter()
            .filter(|c| c.cluster_status == Some("available".to_string()))
            .collect();
        let mut clusters_filtered_by_tags = self.filter_by_tags(&running_clusters);
        let mut clusters_filtered_by_types = self.filter_by_types(&running_clusters);
        let mut idle_clusters = self.filter_by_idle_rules(&running_clusters);

        info!(
            "Redshift Summary: \n\
             \tTotal Redshift Clusters: {} \n\
             \tRunning Redshift Clusters: {} \n\
             \tRedshift Clusters that do not have required tags: {} \n\
             \tRedshift Clusters that are not using the allowed instance-types: {} \n\
             \tRedshift Clusters that are idle: {}",
            clusters.len(),
            running_clusters.len(),
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
