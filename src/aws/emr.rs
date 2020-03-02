use crate::{
    aws::{cloudwatch::CwClient, util, Result},
    config::{EmrConfig, RequiredTags},
    service::{EnforcementState, NTag, NukeService, Resource, ResourceType},
};
use log::{debug, warn};
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use rusoto_ec2::{DescribeSecurityGroupsRequest, Ec2, Ec2Client, Filter};
use rusoto_emr::{
    Cluster, DescribeClusterInput, Emr, EmrClient, ListClustersInput, ListInstancesInput,
    SetTerminationProtectionInput, Tag, TerminateJobFlowsInput,
};

pub struct EmrNukeClient {
    pub client: EmrClient,
    pub cwclient: CwClient,
    pub ec2_client: Option<Ec2Client>,
    pub config: EmrConfig,
    pub region: Region,
    pub dry_run: bool,
}

impl EmrNukeClient {
    pub fn new(
        profile_name: Option<&str>,
        region: Region,
        config: EmrConfig,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(EmrNukeClient {
                client: EmrClient::new_with(HttpClient::new()?, pp.clone(), region.clone()),
                cwclient: CwClient::new(profile_name, region.clone(), config.clone().idle_rules)?,
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
            Ok(EmrNukeClient {
                client: EmrClient::new(region.clone()),
                cwclient: CwClient::new(profile_name, region.clone(), config.clone().idle_rules)?,
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
        clusters: Vec<Cluster>,
        sgs: Option<Vec<String>>,
    ) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for cluster in clusters {
            let cluster_id = cluster.id.as_ref().unwrap().to_owned();

            let enforcement_state = {
                if self.config.ignore.contains(&cluster_id) {
                    debug!("Skipping resource from ignore list - {}", cluster_id);
                    EnforcementState::SkipConfig
                } else if cluster.status.as_ref().unwrap().state != Some("RUNNING".to_string()) {
                    debug!("Skipping resource as its not running - {}", cluster_id);
                    EnforcementState::SkipStopped
                } else {
                    if self.resource_tags_does_not_match(&cluster) {
                        debug!("Resource tags does not match - {}", cluster_id);
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.resource_types_does_not_match(&cluster) {
                        debug!("Resource types does not match - {}", cluster_id);
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.is_resource_idle(&cluster) {
                        debug!("Resource is idle - {}", cluster_id);
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.is_resource_not_secure(&cluster, sgs.clone()) {
                        debug!("Resource is not secure - {}", cluster_id);
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else {
                        EnforcementState::Skip
                    }
                }
            };

            resources.push(Resource {
                id: cluster_id,
                region: self.region.clone(),
                resource_type: ResourceType::EmrCluster,
                tags: self.package_tags_as_ntags(cluster.tags.clone()),
                state: cluster.status.as_ref().unwrap().state.clone(),
                enforcement_state,
            });
        }

        Ok(resources)
    }

    fn resource_tags_does_not_match(&self, cluster: &Cluster) -> bool {
        if self.config.required_tags.is_some() {
            !self.check_tags(&cluster.tags, &self.config.required_tags.as_ref().unwrap())
        } else {
            false
        }
    }

    fn resource_types_does_not_match(&self, cluster: &Cluster) -> bool {
        if !self.config.allowed_instance_types.is_empty() {
            if let Ok(instance_types) = self.get_instance_types(cluster.id.as_ref().unwrap()) {
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

    fn is_resource_idle(&self, _cluster: &Cluster) -> bool {
        // TODO: https://github.com/rusoto/rusoto/issues/1266
        false
    }

    fn is_resource_not_secure(&self, cluster: &Cluster, sgs: Option<Vec<String>>) -> bool {
        if self.config.security_groups.enabled && sgs.is_some() {
            let mut cluster_sgs = Vec::new();

            if let Some(instance_attributes) = cluster.ec_2_instance_attributes.clone() {
                if let Some(master_sg) = instance_attributes.emr_managed_master_security_group {
                    cluster_sgs.push(master_sg.clone());
                }

                if let Some(worker_sg) = instance_attributes.emr_managed_slave_security_group {
                    cluster_sgs.push(worker_sg.clone());
                }

                if let Some(add_master_sgs) = instance_attributes.additional_master_security_groups
                {
                    for add_master_sg in add_master_sgs {
                        cluster_sgs.push(add_master_sg);
                    }
                }

                if let Some(add_worker_sgs) = instance_attributes.additional_slave_security_groups {
                    for add_worker_sg in add_worker_sgs {
                        cluster_sgs.push(add_worker_sg);
                    }
                }

                sgs.unwrap().iter().any(|s| cluster_sgs.contains(&s))
            } else {
                false
            }
        } else {
            false
        }
    }

    fn get_instance_types(&self, cluster_id: &str) -> Result<Vec<String>> {
        let mut instance_types = Vec::new();
        let result = self
            .client
            .list_instances(ListInstancesInput {
                cluster_id: cluster_id.to_owned(),
                ..Default::default()
            })
            .sync()?;

        for instance in result.instances.unwrap_or_default() {
            if let Some(it) = instance.instance_type {
                instance_types.push(it);
            }
        }

        Ok(instance_types)
    }

    /// Checks cluster tags against required tags and returns true only if all required tags are
    /// present
    fn check_tags(&self, tags: &Option<Vec<Tag>>, required_tags: &Vec<RequiredTags>) -> bool {
        let ntags = self.package_tags_as_ntags(tags.to_owned());
        util::compare_tags(ntags, required_tags)
    }

    fn get_clusters(&self) -> Result<Vec<Cluster>> {
        let mut next_token: Option<String> = None;
        let mut clusters: Vec<Cluster> = Vec::new();

        loop {
            let result = self
                .client
                .list_clusters(ListClustersInput {
                    marker: next_token,
                    ..Default::default()
                })
                .sync()?;

            if let Some(cs) = result.clusters {
                for c in cs {
                    // TODO: https://github.com/rusoto/rusoto/issues/1266

                    match self
                        .client
                        .describe_cluster(DescribeClusterInput {
                            cluster_id: c.id.unwrap_or_default(),
                        })
                        .sync()
                    {
                        Ok(result) => {
                            if let Some(cluster) = result.cluster {
                                clusters.push(cluster);
                            }
                        }
                        Err(e) => {
                            warn!("Failed 'describe-cluster'. Err: {:?}", e);
                        }
                    }
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

    fn disable_termination_protection(&self, cluster_ids: Vec<String>) -> Result<()> {
        if !self.dry_run && !cluster_ids.is_empty() {
            self.client
                .set_termination_protection(SetTerminationProtectionInput {
                    job_flow_ids: cluster_ids,
                    termination_protected: false,
                })
                .sync()?
        }

        Ok(())
    }

    fn terminate_resources(&self, cluster_ids: &Vec<String>) -> Result<()> {
        debug!("Terminating the clusters: {:?}", cluster_ids);

        if self.config.termination_protection.ignore {
            self.disable_termination_protection(cluster_ids.to_owned())?;
        }

        if !self.dry_run && !cluster_ids.is_empty() {
            self.client
                .terminate_job_flows(TerminateJobFlowsInput {
                    job_flow_ids: cluster_ids.to_owned(),
                })
                .sync()?;
        }

        Ok(())
    }

    fn get_security_groups(&self) -> Result<Vec<String>> {
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
                .sync()?;

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

impl NukeService for EmrNukeClient {
    fn scan(&self) -> Result<Vec<Resource>> {
        let clusters = self.get_clusters()?;
        let sgs = if self.config.security_groups.enabled {
            Some(self.get_security_groups()?)
        } else {
            None
        };

        Ok(self.package_clusters_as_resources(clusters, sgs)?)
    }

    fn stop(&self, resource: &Resource) -> Result<()> {
        self.terminate_resources(vec![resource.id.to_owned()].as_ref())
    }

    fn delete(&self, resource: &Resource) -> Result<()> {
        self.terminate_resources(vec![resource.id.to_owned()].as_ref())
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
