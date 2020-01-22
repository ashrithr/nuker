use {
    crate::aws::cloudwatch::CwClient,
    crate::config::RdsConfig,
    crate::config::TargetState,
    crate::error::Error as AwsError,
    crate::service::{NTag, NukeService, Resource, ResourceType},
    log::{debug, info, trace},
    rusoto_core::{HttpClient, Region},
    rusoto_credential::ProfileProvider,
    rusoto_rds::{
        DBCluster, DescribeDBClustersMessage, Filter, ListTagsForResourceMessage, Rds, RdsClient,
        Tag,
    },
};

type Result<T, E = AwsError> = std::result::Result<T, E>;

pub struct RdsAuroraNukeClient {
    pub client: RdsClient,
    pub cwclient: CwClient,
    pub config: RdsConfig,
    pub dry_run: bool,
}

impl RdsAuroraNukeClient {
    pub fn new(
        profile_name: &String,
        region: Region,
        config: RdsConfig,
        dry_run: bool,
    ) -> Result<Self> {
        let mut pp = ProfileProvider::new()?;
        pp.set_profile(profile_name);

        Ok(RdsAuroraNukeClient {
            client: RdsClient::new_with(HttpClient::new()?, pp, region.clone()),
            cwclient: CwClient::new(profile_name, region, config.clone().idle_rules)?,
            config: config,
            dry_run: dry_run,
        })
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

    fn package_clusters_as_resources(
        &self,
        profile_name: &String,
        clusters: Vec<&DBCluster>,
    ) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for cluster in clusters {
            let cluster_id = cluster.db_cluster_identifier.as_ref().unwrap().to_owned();

            if let Some(_resource) = resources.iter().find(|r| r.id == cluster_id) {
                trace!("Skipping resource, already exists in the list.");
            } else {
                resources.push(Resource {
                    id: cluster_id,
                    resource_type: ResourceType::RDS,
                    profile_name: profile_name.to_owned(),
                    tags: self
                        .package_tags_as_ntags(self.list_tags(cluster.db_cluster_arn.clone())?),
                    state: cluster.status.clone(),
                });
            }

            std::thread::sleep(std::time::Duration::from_millis(100));
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

            std::thread::sleep(std::time::Duration::from_millis(50));
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
}

impl NukeService for RdsAuroraNukeClient {
    fn scan(&self, profile_name: &String) -> Result<Vec<Resource>> {
        unimplemented!()
    }

    fn cleanup(&self, resources: Vec<&Resource>) -> Result<()> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
