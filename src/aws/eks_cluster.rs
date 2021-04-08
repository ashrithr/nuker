use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_eks::{
    Cluster, DeleteClusterRequest, DeleteFargateProfileRequest, DeleteNodegroupRequest,
    DescribeClusterRequest, DescribeNodegroupRequest, Eks, EksClient, ListClustersRequest,
    ListFargateProfilesRequest, ListNodegroupsRequest,
};
use std::collections::HashMap;
use std::str::FromStr;
use tracing::{debug, trace};

#[derive(Clone)]
pub struct EksClusterClient {
    client: EksClient,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl EksClusterClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        EksClusterClient {
            client: EksClient::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn package_resources(&self, clusters: Vec<Cluster>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for cluster in clusters {
            let cluster_id = cluster.name.as_deref().unwrap();
            let resource_type = self.get_instance_types(cluster_id).await.ok();

            resources.push(Resource {
                id: cluster.name.unwrap(),
                arn: cluster.arn,
                type_: ClientType::EksCluster,
                region: self.region.clone(),
                tags: self.package_tags(cluster.tags),
                state: ResourceState::from_str(cluster.status.as_ref().unwrap()).ok(),
                start_time: None,
                enforcement_state: EnforcementState::SkipUnknownState,
                enforcement_reason: None,
                resource_type,
                dependencies: None,
                termination_protection: None,
            });
        }

        Ok(resources)
    }

    async fn get_clusters(&self) -> Result<Vec<Cluster>> {
        let mut clusters: Vec<Cluster> = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let req = self.client.list_clusters(ListClustersRequest {
                next_token,
                ..Default::default()
            });

            if let Ok(result) = handle_future_with_return!(req) {
                if let Some(cluster_arns) = result.clusters {
                    for cluster_arn in cluster_arns {
                        let req = self.client.describe_cluster(DescribeClusterRequest {
                            name: cluster_arn,
                            ..Default::default()
                        });

                        if let Ok(result) = handle_future_with_return!(req) {
                            if let Some(cluster) = result.cluster {
                                clusters.push(cluster);
                            }
                        }
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

        Ok(clusters)
    }

    async fn get_instance_types(&self, cluster_name: &str) -> Result<Vec<String>> {
        let mut instance_types: Vec<String> = Vec::new();

        let req = self.client.list_nodegroups(ListNodegroupsRequest {
            cluster_name: cluster_name.to_string(),
            ..Default::default()
        });

        if let Ok(result) = handle_future_with_return!(req) {
            if let Some(node_groups) = result.nodegroups {
                for node_group in node_groups {
                    let req = self.client.describe_nodegroup(DescribeNodegroupRequest {
                        cluster_name: cluster_name.to_string(),
                        nodegroup_name: node_group,
                    });

                    if let Ok(result) = handle_future_with_return!(req) {
                        if let Some(ng) = result.nodegroup {
                            if let Some(its) = ng.instance_types {
                                for it in its {
                                    instance_types.push(it);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(instance_types)
    }

    async fn delete_nodegroups(&self, resource: &Resource) -> Result<()> {
        if !self.dry_run {
            let req = self.client.list_nodegroups(ListNodegroupsRequest {
                cluster_name: resource.id.to_string(),
                ..Default::default()
            });

            if let Ok(result) = handle_future_with_return!(req) {
                if let Some(node_groups) = result.nodegroups {
                    for node_group in node_groups {
                        let req = self.client.delete_nodegroup(DeleteNodegroupRequest {
                            cluster_name: resource.id.to_string(),
                            nodegroup_name: node_group,
                        });

                        handle_future!(req);
                    }
                }
            }
        }

        Ok(())
    }

    async fn delete_fargate_profiles(&self, resource: &Resource) -> Result<()> {
        if !self.dry_run {
            let req = self
                .client
                .list_fargate_profiles(ListFargateProfilesRequest {
                    cluster_name: resource.id.to_string(),
                    ..Default::default()
                });

            if let Ok(result) = handle_future_with_return!(req) {
                if let Some(fargate_profiles) = result.fargate_profile_names {
                    for fargate_profile in fargate_profiles {
                        let req = self
                            .client
                            .delete_fargate_profile(DeleteFargateProfileRequest {
                                cluster_name: resource.id.to_string(),
                                fargate_profile_name: fargate_profile,
                            });

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
            self.delete_nodegroups(resource).await?;
            self.delete_fargate_profiles(resource).await?;

            let req = self.client.delete_cluster(DeleteClusterRequest {
                name: resource.id.clone(),
            });
            handle_future!(req);
        }

        Ok(())
    }

    fn package_tags(&self, tags: Option<HashMap<String, String>>) -> Option<Vec<NTag>> {
        tags.map(|ts| {
            ts.iter()
                .map(|(key, value)| NTag {
                    key: Some(key.clone()),
                    value: Some(value.clone()),
                })
                .collect()
        })
    }
}

#[async_trait]
impl NukerClient for EksClusterClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized EKS resource scanner");
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
