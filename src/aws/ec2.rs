use {
    crate::aws::cloudwatch::CwClient,
    crate::aws::Result,
    crate::config::Ec2Config,
    crate::config::TargetState,
    crate::error::Error as AwsError,
    crate::service::{NTag, NukeService, Resource, ResourceType},
    log::{debug, info, trace},
    rusoto_core::{HttpClient, Region},
    rusoto_credential::ProfileProvider,
    rusoto_ec2::{
        DeleteVolumeRequest, DescribeInstanceAttributeRequest, DescribeInstancesRequest,
        DescribeInstancesResult, DescribeSecurityGroupsRequest, DescribeVolumesRequest, Ec2,
        Ec2Client, Filter, Instance, ModifyInstanceAttributeRequest, SecurityGroup,
        StopInstancesRequest, Tag, TerminateInstancesRequest, Volume,
    },
};

pub struct Ec2NukeClient {
    pub client: Ec2Client,
    pub cwclient: CwClient,
    pub config: Ec2Config,
    pub dry_run: bool,
}

impl Ec2NukeClient {
    pub fn new(
        profile_name: Option<&str>,
        region: Region,
        config: Ec2Config,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(Ec2NukeClient {
                client: Ec2Client::new_with(HttpClient::new()?, pp, region.clone()),
                cwclient: CwClient::new(profile_name, region, config.clone().idle_rules)?,
                config,
                dry_run,
            })
        } else {
            Ok(Ec2NukeClient {
                client: Ec2Client::new(region.clone()),
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

    fn package_instances_as_resources(&self, instances: Vec<&Instance>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for instance in instances {
            let instance_id = instance.instance_id.as_ref().unwrap().to_owned();
            // eliminate the duplicates
            if let Some(_resource) = resources.iter().find(|r| r.id == instance_id) {
                trace!("Skipping resource, already exists in the list.");
            } else {
                resources.push(Resource {
                    id: instance_id,
                    resource_type: ResourceType::EC2,
                    tags: self.package_tags_as_ntags(instance.tags.clone()),
                    state: instance.state.as_ref().unwrap().name.clone(),
                });
            }
        }

        Ok(resources)
    }

    fn package_volumes_as_resources(&self, volumes: Vec<Volume>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for volume in volumes {
            let volume_id = volume.volume_id.as_ref().unwrap().to_string();
            if let Some(_resource) = resources.iter().find(|r| r.id == volume_id) {
                trace!("Skipping resource, already exists in the list.")
            } else {
                resources.push(Resource {
                    id: volume_id,
                    resource_type: ResourceType::Volume,
                    tags: self.package_tags_as_ntags(volume.tags.clone()),
                    state: volume.state,
                })
            }
        }

        Ok(resources)
    }

    fn get_instances(&self, filter: Vec<Filter>) -> Result<Vec<Instance>> {
        let mut next_token: Option<String> = None;
        let mut instances: Vec<Instance> = Vec::new();

        loop {
            let result: DescribeInstancesResult = self
                .client
                .describe_instances(DescribeInstancesRequest {
                    dry_run: None,
                    filters: Some(filter.clone()),
                    instance_ids: None,
                    max_results: None,
                    next_token,
                })
                .sync()?;

            if let Some(reservations) = result.reservations {
                let reservations: Vec<Vec<Instance>> = reservations
                    .into_iter()
                    .filter_map(|reservation| reservation.instances)
                    .collect();

                let mut temp_instances: Vec<Instance> =
                    reservations.into_iter().flatten().collect();

                instances.append(&mut temp_instances);
            }

            if result.next_token.is_none() {
                break;
            } else {
                next_token = result.next_token;
            }
        }

        if !self.config.ignore.is_empty() {
            debug!("Ignoring instances {:?}", self.config.ignore);
            instances.retain(|i| !self.config.ignore.contains(&i.instance_id.clone().unwrap()));
        }

        Ok(instances)
    }

    fn filter_by_tags<'a>(&self, instances: &Vec<&'a Instance>) -> Vec<&'a Instance> {
        debug!(
            "Total # of instances before applying Filter by required tags - {:?}: {}.",
            &self.config.required_tags,
            instances.len()
        );

        instances
            .iter()
            .filter(|instance| !self.check_tags(&instance.tags, &self.config.required_tags))
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

    fn filter_by_types<'a>(&self, instances: &Vec<&'a Instance>) -> Vec<&'a Instance> {
        debug!(
            "Total # of instances before applying Filter by Instance type - {:?}: {}",
            self.config.allowed_instance_types,
            instances.len()
        );

        instances
            .iter()
            .filter(|instance| {
                self.config
                    .allowed_instance_types
                    .iter()
                    .any(|it| Some(it) == instance.instance_type.as_ref())
            })
            .cloned()
            .collect()
    }

    fn filter_by_idle_rules<'a>(&self, instances: &Vec<&'a Instance>) -> Vec<&'a Instance> {
        debug!(
            "Total # of instances before applying Filter by CPU Utilization - {:?}: {}",
            self.config.idle_rules,
            instances.len()
        );

        instances
            .iter()
            .filter(|instance| {
                instance.state.as_ref().unwrap().code == Some(16)
                    && !self
                        .cwclient
                        .filter_instance_by_utilization(&instance.instance_id.as_ref().unwrap())
                        .unwrap()
            })
            .cloned()
            .collect()
    }

    fn filter_by_security_groups<'a>(
        &self,
        instances: &Vec<&'a Instance>,
        security_groups: &Vec<String>,
    ) -> Vec<&'a Instance> {
        debug!(
            "Total # of instances before applying Filter by Security Groups - {:?}: {}",
            self.config.security_groups,
            instances.len()
        );

        instances
            .iter()
            .filter(|i| {
                security_groups.iter().any(|s| {
                    i.security_groups
                        .as_ref()
                        .unwrap()
                        .iter()
                        .map(|gi| gi.group_id.as_ref().unwrap())
                        .collect::<Vec<&String>>()
                        .iter()
                        .any(|is| &s == is)
                })
            })
            .cloned()
            .collect()
    }

    fn disable_termination_protection(&self, instance_id: &str) -> Result<()> {
        let resp = self
            .client
            .describe_instance_attribute(DescribeInstanceAttributeRequest {
                attribute: "disableApiTermination".into(),
                instance_id: instance_id.into(),
                ..Default::default()
            })
            .sync()?;

        if resp.disable_api_termination.unwrap().value.unwrap() {
            debug!(
                "Terminating protection was enabled for: {}. Trying to Disable it.",
                instance_id
            );

            if !self.dry_run {
                self.client
                    .modify_instance_attribute(ModifyInstanceAttributeRequest {
                        attribute: Some("disableApiTermination".into()),
                        instance_id: instance_id.into(),
                        ..Default::default()
                    })
                    .sync()?;
            }
        }

        Ok(())
    }

    fn terminate_instances(&self, instance_ids: &Vec<String>) -> Result<()> {
        debug!("Terminating the instances: {:?}", instance_ids);

        if self.config.termination_protection.ignore {
            for instance_id in instance_ids {
                self.disable_termination_protection(instance_id)?;
            }
        }

        if !self.dry_run {
            if !instance_ids.is_empty() {
                self.client
                    .terminate_instances(TerminateInstancesRequest {
                        instance_ids: instance_ids.to_owned(),
                        ..Default::default()
                    })
                    .sync()?;
            }
        }

        Ok(())
    }

    fn stop_instances(&self, instance_ids: &Vec<String>) -> Result<()> {
        debug!("Stopping instances: {:?}", instance_ids);

        if !self.dry_run {
            if !instance_ids.is_empty() {
                self.client
                    .stop_instances(StopInstancesRequest {
                        instance_ids: instance_ids.to_owned(),
                        force: Some(true),
                        ..Default::default()
                    })
                    .sync()?;
            }
        }

        Ok(())
    }

    fn get_volumes(&self) -> Result<Vec<Volume>> {
        let mut next_token: Option<String> = None;
        let mut volumes: Vec<Volume> = Vec::new();

        loop {
            let result = self
                .client
                .describe_volumes(DescribeVolumesRequest {
                    filters: Some(vec![Filter {
                        name: Some("status".to_string()),
                        values: Some(vec!["available".to_string()]),
                    }]),
                    next_token: next_token,
                    ..Default::default()
                })
                .sync()?;

            if let Some(vs) = result.volumes {
                for v in vs {
                    volumes.push(v);
                }
            }

            if result.next_token.is_none() {
                break;
            } else {
                next_token = result.next_token;
            }
        }

        Ok(volumes)
    }

    fn delete_volumes(&self, volume_ids: &Vec<String>) -> Result<()> {
        info!("Deleting Volumes: {:?}", volume_ids);

        for volume_id in volume_ids {
            self.client.delete_volume(DeleteVolumeRequest {
                volume_id: volume_id.to_owned(),
                ..Default::default()
            });
        }

        Ok(())
    }

    fn get_security_groups(&self) -> Result<Vec<String>> {
        let mut next_token: Option<String> = None;
        let mut security_groups: Vec<String> = Vec::new();

        loop {
            let result = self
                .client
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

impl NukeService for Ec2NukeClient {
    fn scan(&self) -> Result<Vec<Resource>> {
        let instances = self.get_instances(Vec::new())?;
        let sgs = if self.config.security_groups.enabled {
            self.get_security_groups()?
        } else {
            Vec::new()
        };

        let mut filtered_instances: Vec<&Instance> = Vec::new();
        let mut scanned_resources: Vec<Resource> = Vec::new();

        let running_instances: Vec<&Instance> = instances
            .iter()
            .filter(|i| i.state.as_ref().unwrap().code == Some(16))
            .collect();
        let stopped_instances: Vec<&Instance> = instances
            .iter()
            .filter(|i| i.state.as_ref().unwrap().code == Some(80))
            .collect();

        let mut instances_filtered_by_tags = self.filter_by_tags(&running_instances);
        debug!(
            "Instances filtered by tags {:?}",
            instances_filtered_by_tags
                .iter()
                .map(|i| i.instance_id.as_ref())
                .collect::<Vec<Option<&String>>>()
        );

        let mut instances_filtered_by_types = self.filter_by_types(&running_instances);
        debug!(
            "Instances filtered by types {:?}",
            instances_filtered_by_types
                .iter()
                .map(|i| i.instance_id.as_ref())
                .collect::<Vec<Option<&String>>>()
        );

        let mut idle_instances = self.filter_by_idle_rules(&running_instances);
        debug!(
            "Instances filtered by Idle Rules {:?}",
            idle_instances
                .iter()
                .map(|i| i.instance_id.as_ref())
                .collect::<Vec<Option<&String>>>()
        );

        let mut instances_filtered_by_sgs = if self.config.security_groups.enabled {
            self.filter_by_security_groups(&running_instances, &sgs)
        } else {
            Vec::default()
        };
        debug!(
            "Instances filtered by Security Group Rules {:?}",
            instances_filtered_by_sgs
                .iter()
                .map(|i| i.instance_id.as_ref())
                .collect::<Vec<Option<&String>>>()
        );

        info!(
            "Instance Summary: \n\
             \tTotal Instances: {} \n\
             \tRunning Instances: {} \n\
             \tStopped Instances: {} \n\
             \tInstances that do not have required tags: {} \n\
             \tInstances that are not using the allowed instance-types: {} \n\
             \tInstances that are idle: {} \n\
             \tInstances that are filtered by security groups: {}",
            instances.len(),
            running_instances.len(),
            stopped_instances.len(),
            instances_filtered_by_tags.len(),
            instances_filtered_by_types.len(),
            idle_instances.len(),
            instances_filtered_by_sgs.len(),
        );

        filtered_instances.append(&mut instances_filtered_by_tags);
        filtered_instances.append(&mut instances_filtered_by_types);
        filtered_instances.append(&mut idle_instances);
        filtered_instances.append(&mut instances_filtered_by_sgs);

        if self.config.ebs_cleanup.enabled {
            scanned_resources.append(&mut self.package_volumes_as_resources(self.get_volumes()?)?);
        }

        scanned_resources.append(&mut self.package_instances_as_resources(filtered_instances)?);

        Ok(scanned_resources)
    }

    fn cleanup(&self, resources: Vec<&Resource>) -> Result<()> {
        let instance_ids = resources
            .clone()
            .into_iter()
            .filter(|r| r.resource_type.is_instance())
            .map(|r| r.id.clone())
            .collect::<Vec<String>>();

        match self.config.target_state {
            TargetState::Stopped => self.stop_instances(&instance_ids)?,
            TargetState::Terminated | TargetState::Deleted => {
                self.terminate_instances(&instance_ids)?
            }
        }

        if self.config.ebs_cleanup.enabled {
            let vol_ids = resources
                .into_iter()
                .filter(|r| r.resource_type.is_volume())
                .map(|r| r.id.clone())
                .collect::<Vec<String>>();

            self.delete_volumes(&vol_ids)?;
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
