use crate::aws::cloudwatch::CwClient;
use crate::aws::{util, Result};
use crate::config::{EcsConfig, RequiredTags};
use crate::resource::{EnforcementState, NTag, Resource, ResourceType};
use crate::service::NukerService;
use async_trait::async_trait;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use rusoto_ecs::{
    Cluster, DeleteClusterRequest, DeregisterContainerInstanceRequest, DescribeClustersRequest,
    Ecs, EcsClient, ListAttributesRequest, ListClustersRequest, ListContainerInstancesRequest,
    ListTagsForResourceRequest, Tag,
};
use std::sync::Arc;
use tracing::{debug, trace};

#[derive(Clone)]
pub struct EcsService {
    pub client: EcsClient,
    pub cw_client: Arc<Box<CwClient>>,
    pub config: EcsConfig,
    pub region: Region,
    pub dry_run: bool,
}

impl EcsService {
    pub fn new(
        profile_name: Option<String>,
        region: Region,
        config: EcsConfig,
        cw_client: Arc<Box<CwClient>>,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = &profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(EcsService {
                client: EcsClient::new_with(HttpClient::new()?, pp, region.clone()),
                cw_client,
                config,
                region,
                dry_run,
            })
        } else {
            Ok(EcsService {
                client: EcsClient::new(region.clone()),
                cw_client,
                config,
                region,
                dry_run,
            })
        }
    }

    async fn package_clusters_as_resources(&self, clusters: Vec<Cluster>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for cluster in clusters {
            let cluster_name = cluster.cluster_name.as_ref().unwrap().clone();
            let tags = cluster.tags.clone();

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&cluster_name) {
                    debug!(
                        resource = cluster_name.as_str(),
                        "Skipping resource from ignore list"
                    );
                    EnforcementState::SkipConfig
                } else {
                    if self.resource_tags_does_not_match(&tags).await {
                        debug!(
                            resource = cluster_name.as_str(),
                            "ECS Cluster tags does not match"
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.resource_types_does_not_match(&cluster_name).await? {
                        debug!(
                            resource = cluster_name.as_str(),
                            "ECS Cluster Instance types does not match"
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.is_resource_idle(&cluster_name).await {
                        debug!(resource = cluster_name.as_str(), "ECS Cluster is idle");
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else {
                        EnforcementState::Skip
                    }
                }
            };

            resources.push(Resource {
                id: cluster_name,
                arn: cluster.cluster_arn,
                resource_type: ResourceType::EcsCluster,
                region: self.region.clone(),
                tags: self.package_tags_as_ntags(tags),
                state: cluster.status,
                enforcement_state,
                dependencies: None,
            });
        }

        Ok(resources)
    }

    async fn resource_tags_does_not_match(&self, tags: &Option<Vec<Tag>>) -> bool {
        if self.config.required_tags.is_some() {
            !self.check_tags(tags, &self.config.required_tags.as_ref().unwrap())
        } else {
            false
        }
    }

    async fn resource_types_does_not_match(&self, cluster_name: &String) -> Result<bool> {
        if !self.config.allowed_instance_types.is_empty() {
            let instance_types: Vec<String> = self.get_instance_types(cluster_name).await?;

            Ok(instance_types
                .iter()
                .any(|it| !self.config.allowed_instance_types.contains(&it)))
        } else {
            Ok(false)
        }
    }

    async fn is_resource_idle(&self, cluster_name: &String) -> bool {
        if self.config.idle_rules.is_some() {
            !self
                .cw_client
                .filter_ecs_cluster(&cluster_name)
                .await
                .unwrap()
        } else {
            false
        }
    }

    async fn get_clusters(&self) -> Result<Vec<Cluster>> {
        let mut _clusters: Vec<String> = Vec::new();
        let mut clusters: Vec<Cluster> = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let result = self
                .client
                .list_clusters(ListClustersRequest {
                    next_token,
                    ..Default::default()
                })
                .await?;

            if let Some(cluster_arns) = result.cluster_arns {
                for cluster_arn in cluster_arns {
                    _clusters.push(cluster_arn);
                }
            }

            if result.next_token.is_none() {
                break;
            } else {
                next_token = result.next_token;
            }
        }

        let result = self
            .client
            .describe_clusters(DescribeClustersRequest {
                clusters: Some(_clusters),
                ..Default::default()
            })
            .await?
            .clusters;

        if let Some(cs) = result {
            for mut c in cs {
                c.tags = self.get_tags(&c.cluster_arn.as_ref().unwrap()).await?;
                clusters.push(c);
            }
        }

        Ok(clusters)
    }

    async fn get_tags(&self, cluster_arn: &String) -> Result<Option<Vec<Tag>>> {
        Ok(self
            .client
            .list_tags_for_resource(ListTagsForResourceRequest {
                resource_arn: cluster_arn.to_string(),
            })
            .await?
            .tags)
    }

    async fn get_instance_types(&self, cluster_name: &String) -> Result<Vec<String>> {
        let mut instance_types: Vec<String> = Vec::new();

        let result = self
            .client
            .list_attributes(ListAttributesRequest {
                target_type: "container-instance".to_string(),
                cluster: Some(cluster_name.to_string()),
                attribute_name: Some("ecs.instance-type".to_string()),
                ..Default::default()
            })
            .await?;

        if let Some(attributes) = result.attributes {
            for attribute in attributes {
                if let Some(instance_type) = attribute.value {
                    instance_types.push(instance_type);
                }
            }
        }

        Ok(instance_types)
    }

    async fn deregister_instnaces(&self, resource: &Resource) -> Result<()> {
        if !self.dry_run {
            let result = self
                .client
                .list_container_instances(ListContainerInstancesRequest {
                    cluster: resource.arn.clone(),
                    ..Default::default()
                })
                .await?;

            if let Some(instance_arns) = result.container_instance_arns {
                for instance_arn in instance_arns {
                    self.client
                        .deregister_container_instance(DeregisterContainerInstanceRequest {
                            container_instance: instance_arn,
                            cluster: resource.arn.clone(),
                            force: Some(true),
                        })
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn delete_cluster(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting");

        if !self.dry_run {
            self.deregister_instnaces(resource).await?;

            self.client
                .delete_cluster(DeleteClusterRequest {
                    cluster: resource.id.clone(),
                })
                .await?;
        }

        Ok(())
    }

    fn check_tags(&self, tags: &Option<Vec<Tag>>, required_tags: &Vec<RequiredTags>) -> bool {
        let ntags = self.package_tags_as_ntags(tags.to_owned());
        util::compare_tags(ntags, required_tags)
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
}

#[async_trait]
impl NukerService for EcsService {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized ECS resource scanner");
        let clusters = self.get_clusters().await?;

        Ok(self.package_clusters_as_resources(clusters).await?)
    }

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.delete_cluster(resource).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_cluster(resource).await
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
