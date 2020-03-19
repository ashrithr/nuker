use crate::{
    aws::{cloudwatch::CwClient, util, Result},
    config::EmrConfig,
    resource::{EnforcementState, Resource, ResourceType},
    service::NukerService,
};
use async_trait::async_trait;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use rusoto_ec2::{DescribeSecurityGroupsRequest, Ec2, Ec2Client, Filter};
use rusoto_emr::{
    ClusterSummary, Emr, EmrClient, ListClustersInput, ListInstancesInput,
    SetTerminationProtectionInput, TerminateJobFlowsInput,
};
use std::sync::Arc;
use tracing::{debug, trace};

#[derive(Clone)]
pub struct EmrService {
    pub client: EmrClient,
    pub cw_client: Arc<Box<CwClient>>,
    pub ec2_client: Option<Ec2Client>,
    pub config: EmrConfig,
    pub region: Region,
    pub dry_run: bool,
}

impl EmrService {
    pub fn new(
        profile_name: Option<String>,
        region: Region,
        config: EmrConfig,
        cw_client: Arc<Box<CwClient>>,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = &profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(EmrService {
                client: EmrClient::new_with(HttpClient::new()?, pp.clone(), region.clone()),
                cw_client,
                ec2_client: if config.security_groups.enabled {
                    Some(Ec2Client::new_with(HttpClient::new()?, pp, region.clone()))
                } else {
                    None
                },
                config,
                region,
                dry_run,
            })
        } else {
            Ok(EmrService {
                client: EmrClient::new(region.clone()),
                cw_client,
                ec2_client: if config.security_groups.enabled {
                    Some(Ec2Client::new(region.clone()))
                } else {
                    None
                },
                config,
                region,
                dry_run,
            })
        }
    }

    async fn package_clusters_as_resources(
        &self,
        clusters: Vec<ClusterSummary>,
        sgs: Option<Vec<String>>,
    ) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for cluster in clusters {
            let cluster_id = cluster.id.as_ref().unwrap().to_owned();

            let enforcement_state = {
                if self.config.ignore.contains(&cluster_id) {
                    debug!(
                        resource = cluster_id.as_str(),
                        "Skipping resource from ignore list"
                    );
                    EnforcementState::SkipConfig
                } else if !(cluster.status.as_ref().unwrap().state == Some("RUNNING".to_string())
                    || cluster.status.as_ref().unwrap().state == Some("WAITING".to_string()))
                {
                    debug!(
                        resource = cluster_id.as_str(),
                        "Skipping resource as its not running"
                    );
                    EnforcementState::SkipUnknownState
                } else {
                    if self.resource_tags_does_not_match(&cluster) {
                        debug!(
                            resource = cluster_id.as_str(),
                            "Resource tags does not match"
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.resource_types_does_not_match(&cluster).await {
                        debug!(
                            resource = cluster_id.as_str(),
                            "Resource types does not match"
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.resource_allowed_run_time(&cluster) {
                        debug!(
                            resource = cluster_id.as_str(),
                            "Cluster is running beyond allowed runtime of {:?}",
                            self.config.older_than
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.is_resource_idle(&cluster).await {
                        debug!(resource = cluster_id.as_str(), "Resource is idle");
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.is_resource_not_secure(&cluster, sgs.clone()) {
                        debug!(resource = cluster_id.as_str(), "Resource is not secure");
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else {
                        EnforcementState::Skip
                    }
                }
            };

            resources.push(Resource {
                id: cluster_id,
                arn: cluster.cluster_arn,
                region: self.region.clone(),
                resource_type: ResourceType::EmrCluster,
                tags: None,
                state: cluster.status.as_ref().unwrap().state.clone(),
                enforcement_state,
            });
        }

        Ok(resources)
    }

    fn resource_allowed_run_time(&self, cluster: &ClusterSummary) -> bool {
        if self.config.older_than.as_secs() > 0 && cluster.status.is_some() {
            if let Some(timeline) = cluster.status.as_ref().unwrap().timeline.as_ref() {
                let date = format!("{}", timeline.creation_date_time.unwrap_or(0f64) as i64);

                util::is_ts_older_than(date.as_str(), &self.config.older_than)
            } else {
                false
            }
        } else {
            false
        }
    }

    fn resource_tags_does_not_match(&self, _cluster: &ClusterSummary) -> bool {
        // TODO: https://github.com/rusoto/rusoto/issues/1266
        false
    }

    async fn resource_types_does_not_match(&self, cluster: &ClusterSummary) -> bool {
        if !self.config.allowed_instance_types.is_empty() {
            if let Ok(instance_types) = self.get_instance_types(cluster.id.as_ref().unwrap()).await
            {
                if instance_types
                    .iter()
                    .any(|it| self.config.allowed_instance_types.contains(&it))
                {
                    return false;
                }
            }
            false
        } else {
            false
        }
    }

    async fn is_resource_idle(&self, cluster: &ClusterSummary) -> bool {
        if self.config.idle_rules.is_some() {
            !self
                .cw_client
                .filter_emr_cluster(&cluster.id.as_ref().unwrap())
                .await
                .unwrap()
        } else {
            false
        }
    }

    fn is_resource_not_secure(&self, _cluster: &ClusterSummary, _sgs: Option<Vec<String>>) -> bool {
        // TODO: https://github.com/rusoto/rusoto/issues/1266
        false
        // if self.config.security_groups.enabled && sgs.is_some() {
        //     let mut cluster_sgs = Vec::new();
        //
        //     if let Some(instance_attributes) = cluster.ec_2_instance_attributes.clone() {
        //         if let Some(master_sg) = instance_attributes.emr_managed_master_security_group {
        //             cluster_sgs.push(master_sg.clone());
        //         }
        //
        //         if let Some(worker_sg) = instance_attributes.emr_managed_slave_security_group {
        //             cluster_sgs.push(worker_sg.clone());
        //         }
        //
        //         if let Some(add_master_sgs) = instance_attributes.additional_master_security_groups
        //         {
        //             for add_master_sg in add_master_sgs {
        //                 cluster_sgs.push(add_master_sg);
        //             }
        //         }
        //
        //         if let Some(add_worker_sgs) = instance_attributes.additional_slave_security_groups {
        //             for add_worker_sg in add_worker_sgs {
        //                 cluster_sgs.push(add_worker_sg);
        //             }
        //         }
        //
        //         sgs.unwrap().iter().any(|s| cluster_sgs.contains(&s))
        //     } else {
        //         false
        //     }
        // } else {
        //     false
        // }
    }

    async fn get_instance_types(&self, cluster_id: &str) -> Result<Vec<String>> {
        let mut instance_types = Vec::new();
        let result = self
            .client
            .list_instances(ListInstancesInput {
                cluster_id: cluster_id.to_owned(),
                ..Default::default()
            })
            .await?;

        for instance in result.instances.unwrap_or_default() {
            if let Some(it) = instance.instance_type {
                instance_types.push(it);
            }
        }

        Ok(instance_types)
    }

    async fn get_clusters(&self) -> Result<Vec<ClusterSummary>> {
        let mut next_token: Option<String> = None;
        let mut clusters: Vec<ClusterSummary> = Vec::new();

        loop {
            let result = self
                .client
                .list_clusters(ListClustersInput {
                    marker: next_token,
                    ..Default::default()
                })
                .await?;

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
        }

        Ok(clusters)
    }

    async fn disable_termination_protection(&self, cluster_ids: Vec<String>) -> Result<()> {
        if !self.dry_run && !cluster_ids.is_empty() {
            self.client
                .set_termination_protection(SetTerminationProtectionInput {
                    job_flow_ids: cluster_ids,
                    termination_protected: false,
                })
                .await?
        }

        Ok(())
    }

    async fn terminate_resources(&self, cluster_ids: &Vec<String>) -> Result<()> {
        if self.config.termination_protection.ignore {
            self.disable_termination_protection(cluster_ids.to_owned())
                .await?;
        }

        if !self.dry_run && !cluster_ids.is_empty() {
            self.client
                .terminate_job_flows(TerminateJobFlowsInput {
                    job_flow_ids: cluster_ids.to_owned(),
                })
                .await?;
        }

        Ok(())
    }

    async fn get_security_groups(&self) -> Result<Vec<String>> {
        let mut next_token: Option<String> = None;
        let mut security_groups: Vec<String> = Vec::new();
        let ec2_client = self.ec2_client.as_ref().unwrap();

        loop {
            let result = ec2_client
                .describe_security_groups(DescribeSecurityGroupsRequest {
                    filters: Some(vec![
                        Filter {
                            name: Some("ip-permission.cidr".to_string()),
                            values: Some(self.config.security_groups.source_cidr.clone()),
                        },
                        Filter {
                            name: Some("ip-permission.from-port".to_string()),
                            values: Some(vec![self.config.security_groups.from_port.to_string()]),
                        },
                        Filter {
                            name: Some("ip-permission.to-port".to_string()),
                            values: Some(vec![self.config.security_groups.to_port.to_string()]),
                        },
                    ]),
                    next_token,
                    ..Default::default()
                })
                .await?;

            if let Some(sgs) = result.security_groups {
                for sg in sgs {
                    security_groups.push(sg.group_id.unwrap_or_default())
                }
            }

            if result.next_token.is_none() {
                break;
            } else {
                next_token = result.next_token;
            }
        }

        Ok(security_groups)
    }
}

#[async_trait]
impl NukerService for EmrService {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized Emr resource scanner");

        let clusters = self.get_clusters().await?;
        let sgs = if self.config.security_groups.enabled {
            Some(self.get_security_groups().await?)
        } else {
            None
        };

        Ok(self.package_clusters_as_resources(clusters, sgs).await?)
    }

    async fn stop(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting");
        self.terminate_resources(vec![resource.id.to_owned()].as_ref())
            .await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting");
        self.terminate_resources(vec![resource.id.to_owned()].as_ref())
            .await
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
