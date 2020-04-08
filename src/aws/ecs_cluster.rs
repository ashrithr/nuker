use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_ecs::{
    Cluster, DeleteClusterRequest, DeregisterContainerInstanceRequest, DescribeClustersRequest,
    Ecs, EcsClient, ListAttributesRequest, ListClustersRequest, ListContainerInstancesRequest,
    ListTagsForResourceRequest, Tag,
};
use std::str::FromStr;
use tracing::{debug, trace};

#[derive(Clone)]
pub struct EcsClusterClient {
    client: EcsClient,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl EcsClusterClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        EcsClusterClient {
            client: EcsClient::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn package_resources(&self, clusters: Vec<Cluster>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for cluster in clusters {
            let cluster_id = cluster.cluster_name.as_deref().unwrap();
            let resource_type = self.get_instance_types(cluster_id).await.ok();

            resources.push(Resource {
                id: cluster.cluster_name.unwrap(),
                arn: cluster.cluster_arn,
                type_: ClientType::EcsCluster,
                region: self.region.clone(),
                tags: self.package_tags(cluster.tags),
                state: ResourceState::from_str(cluster.status.as_ref().unwrap()).ok(),
                start_time: None,
                enforcement_state: EnforcementState::SkipUnknownState,
                resource_type,
                dependencies: None,
                termination_protection: None,
            });
        }

        Ok(resources)
    }

    async fn get_clusters(&self) -> Result<Vec<Cluster>> {
        let mut _clusters: Vec<String> = Vec::new();
        let mut clusters: Vec<Cluster> = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let req = self.client.list_clusters(ListClustersRequest {
                next_token,
                ..Default::default()
            });

            if let Ok(result) = handle_future_with_return!(req) {
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
            } else {
                break;
            }
        }

        if _clusters.len() > 0 {
            let req = self.client.describe_clusters(DescribeClustersRequest {
                clusters: Some(_clusters),
                ..Default::default()
            });

            if let Ok(result) = handle_future_with_return!(req) {
                if let Some(cs) = result.clusters {
                    for mut c in cs {
                        c.tags = self.get_tags(&c.cluster_arn.as_ref().unwrap()).await;
                        clusters.push(c);
                    }
                }
            }
        }

        Ok(clusters)
    }

    async fn get_tags(&self, cluster_arn: &String) -> Option<Vec<Tag>> {
        let req = self
            .client
            .list_tags_for_resource(ListTagsForResourceRequest {
                resource_arn: cluster_arn.to_string(),
            });

        if let Ok(result) = handle_future_with_return!(req) {
            result.tags
        } else {
            None
        }
    }

    async fn get_instance_types(&self, cluster_name: &str) -> Result<Vec<String>> {
        let mut instance_types: Vec<String> = Vec::new();

        let req = self.client.list_attributes(ListAttributesRequest {
            target_type: "container-instance".to_string(),
            cluster: Some(cluster_name.to_string()),
            attribute_name: Some("ecs.instance-type".to_string()),
            ..Default::default()
        });

        if let Ok(result) = handle_future_with_return!(req) {
            if let Some(attributes) = result.attributes {
                for attribute in attributes {
                    if let Some(instance_type) = attribute.value {
                        instance_types.push(instance_type);
                    }
                }
            }
        }

        Ok(instance_types)
    }

    async fn deregister_instances(&self, resource: &Resource) -> Result<()> {
        if !self.dry_run {
            let req = self
                .client
                .list_container_instances(ListContainerInstancesRequest {
                    cluster: resource.arn.clone(),
                    ..Default::default()
                });
            if let Ok(result) = handle_future_with_return!(req) {
                if let Some(instance_arns) = result.container_instance_arns {
                    for instance_arn in instance_arns {
                        let req = self.client.deregister_container_instance(
                            DeregisterContainerInstanceRequest {
                                container_instance: instance_arn,
                                cluster: resource.arn.clone(),
                                force: Some(true),
                            },
                        );
                        handle_future!(req);
                    }
                }
            }
        }

        Ok(())
    }

    async fn delete_cluster(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting");

        if !self.dry_run {
            self.deregister_instances(resource).await?;

            let req = self.client.delete_cluster(DeleteClusterRequest {
                cluster: resource.id.clone(),
            });
            handle_future!(req);
        }

        Ok(())
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
}

#[async_trait]
impl NukerClient for EcsClusterClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized ECS resource scanner");
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

    async fn stop(&self, _resource: &Resource) -> Result<()> {
        Ok(())
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_cluster(resource).await
    }
}
