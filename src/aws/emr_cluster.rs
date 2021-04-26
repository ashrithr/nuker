use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_emr::{
    ClusterSummary, Emr, EmrClient, ListClustersInput, ListInstancesInput,
    SetTerminationProtectionInput, TerminateJobFlowsInput,
};
use std::str::FromStr;
use tracing::{debug, trace};

#[derive(Clone)]
pub struct EmrClusterClient {
    client: EmrClient,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl EmrClusterClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        EmrClusterClient {
            client: EmrClient::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn package_resources(&self, clusters: Vec<ClusterSummary>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for cluster in clusters {
            let start_time =
                if let Some(timeline) = cluster.status.as_ref().unwrap().timeline.as_ref() {
                    Some(format!(
                        "{}",
                        timeline.creation_date_time.unwrap_or(0f64) as i64
                    ))
                } else {
                    None
                };
            let instance_types = self
                .get_instance_types(cluster.id.as_ref().unwrap())
                .await
                .ok();

            resources.push(Resource {
                id: cluster.id.unwrap(),
                arn: cluster.cluster_arn,
                type_: ClientType::EmrCluster,
                region: self.region.clone(),
                tags: None, // TODO: https://github.com/rusoto/rusoto/issues/1266
                state: ResourceState::from_str(
                    cluster.status.as_ref().unwrap().state.as_ref().unwrap(),
                )
                .ok(),
                start_time,
                enforcement_state: EnforcementState::SkipUnknownState,
                enforcement_reason: None,
                resource_type: instance_types,
                dependencies: None,
                termination_protection: None,
            });
        }

        Ok(resources)
    }

    async fn get_instance_types(&self, cluster_id: &str) -> Result<Vec<String>> {
        let mut instance_types = Vec::new();
        let req = self.client.list_instances(ListInstancesInput {
            cluster_id: cluster_id.to_owned(),
            ..Default::default()
        });

        if let Ok(result) = handle_future_with_return!(req) {
            for instance in result.instances.unwrap_or_default() {
                if let Some(it) = instance.instance_type {
                    instance_types.push(it);
                }
            }
        }

        Ok(instance_types)
    }

    async fn get_clusters(&self) -> Result<Vec<ClusterSummary>> {
        let mut next_token: Option<String> = None;
        let mut clusters: Vec<ClusterSummary> = Vec::new();

        loop {
            let req = self.client.list_clusters(ListClustersInput {
                marker: next_token,
                ..Default::default()
            });

            if let Ok(result) = handle_future_with_return!(req) {
                if let Some(cs) = result.clusters {
                    for c in cs {
                        clusters.push(c);
                        //     // TODO: https://github.com/rusoto/rusoto/issues/1266
                        //
                        //     match self
                        //         .client
                        //         .describe_cluster(DescribeClusterInput {
                        //             cluster_id: c.id.unwrap_or_default(),
                        //         })
                        //         .await
                        //     {
                        //         Ok(result) => {
                        //             if let Some(cluster) = result.cluster {
                        //                 clusters.push(cluster);
                        //             }
                        //         }
                        //         Err(e) => {
                        //             warn!("Failed 'describe-cluster'. Err: {:?}", e);
                        //         }
                        //     }
                    }
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

    async fn disable_termination_protection(&self, cluster_ids: Vec<String>) -> Result<()> {
        if !self.dry_run && !cluster_ids.is_empty() {
            let req = self
                .client
                .set_termination_protection(SetTerminationProtectionInput {
                    job_flow_ids: cluster_ids,
                    termination_protected: false,
                });
            handle_future!(req);
        }

        Ok(())
    }

    async fn terminate_cluster(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting");

        if !self.dry_run {
            if let Some(termination_protection) = self.config.termination_protection.as_ref() {
                if termination_protection.ignore {
                    self.disable_termination_protection(vec![resource.id.to_owned()])
                        .await?;
                }
            }

            let req = self.client.terminate_job_flows(TerminateJobFlowsInput {
                job_flow_ids: vec![resource.id.to_owned()],
            });
            handle_future!(req);
        }

        Ok(())
    }
}

#[async_trait]
impl NukerClient for EmrClusterClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized Emr resource scanner");

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
        self.terminate_cluster(resource).await
    }
}
