use {
    crate::aws::cloudwatch::CwClient,
    crate::aws::Result,
    crate::config::Ec2Config,
    crate::config::TargetState,
    crate::service::{EnforcementState, NTag, NukeService, Resource, ResourceType},
    log::debug,
    rusoto_core::{HttpClient, Region},
    rusoto_credential::ProfileProvider,
    rusoto_ec2::{
        DescribeInstanceAttributeRequest, DescribeInstancesRequest, DescribeInstancesResult,
        DescribeSecurityGroupsRequest, Ec2, Ec2Client, Filter, Instance,
        ModifyInstanceAttributeRequest, StopInstancesRequest, Tag, TerminateInstancesRequest,
    },
};

pub struct Ec2NukeClient {
    pub client: Ec2Client,
    pub cwclient: CwClient,
    pub config: Ec2Config,
    pub region: Region,
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
                cwclient: CwClient::new(profile_name, region.clone(), config.clone().idle_rules)?,
                config,
                region,
                dry_run,
            })
        } else {
            Ok(Ec2NukeClient {
                client: Ec2Client::new(region.clone()),
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

    fn package_instances_as_resources(&self, instances: Vec<Instance>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();
        let sgs = if self.config.security_groups.enabled {
            Some(self.get_security_groups()?)
        } else {
            None
        };

        for instance in instances {
            let instance_id = instance.instance_id.as_ref().unwrap().to_owned();

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&instance_id) {
                    // Instance not in ignore list
                    EnforcementState::SkipConfig
                } else if instance.state.as_ref().unwrap().code != Some(16) {
                    // Instance not in running state
                    EnforcementState::Skip
                } else {
                    if self.resource_tags_does_not_match(&instance)
                        || self.resource_types_does_not_match(&instance)
                        || self.is_resource_idle(&instance)
                        || self.is_resource_secure(&instance, sgs.clone())
                    {
                        if self.config.target_state == TargetState::Deleted {
                            EnforcementState::Delete
                        } else {
                            EnforcementState::Stop
                        }
                    } else {
                        EnforcementState::Skip
                    }
                }
            };

            resources.push(Resource {
                id: instance_id,
                region: self.region.clone(),
                resource_type: ResourceType::Ec2Instance,
                tags: self.package_tags_as_ntags(instance.tags.clone()),
                state: instance.state.as_ref().unwrap().name.clone(),
                enforcement_state,
            });
        }

        Ok(resources)
    }

    fn resource_tags_does_not_match(&self, instance: &Instance) -> bool {
        !self.check_tags(&instance.tags, &self.config.required_tags)
    }

    fn resource_types_does_not_match(&self, instance: &Instance) -> bool {
        !self
            .config
            .allowed_instance_types
            .contains(&instance.instance_type.clone().unwrap())
    }

    fn is_resource_idle(&self, instance: &Instance) -> bool {
        !self
            .cwclient
            .filter_instance_by_utilization(&instance.instance_id.as_ref().unwrap())
            .unwrap()
    }

    fn is_resource_secure(&self, instance: &Instance, sgs: Option<Vec<String>>) -> bool {
        if self.config.security_groups.enabled && sgs.is_some() {
            let instance_sgs = instance
                .security_groups
                .clone()
                .unwrap()
                .iter()
                .map(|gi| gi.group_id.clone().unwrap())
                .collect::<Vec<String>>();

            !sgs.unwrap().iter().any(|s| instance_sgs.contains(&s))
        } else {
            true
        }
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

        Ok(instances)
    }

    /// Checks instance tags against required tags and returns true only if all required tags are
    /// present
    fn check_tags(&self, tags: &Option<Vec<Tag>>, required_tags: &Vec<String>) -> bool {
        let tags: Vec<String> = tags
            .clone()
            .unwrap_or_default()
            .iter()
            .map(|t| t.key.clone().unwrap())
            .collect();
        required_tags.iter().all(|rt| tags.contains(rt))
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

    fn terminate_resources(&self, instance_ids: &Vec<String>) -> Result<()> {
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

    fn stop_resources(&self, instance_ids: &Vec<String>) -> Result<()> {
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

        Ok(self.package_instances_as_resources(instances)?)
    }

    fn stop(&self, resource: &Resource) -> Result<()> {
        self.stop_resources(vec![resource.id.to_owned()].as_ref())
    }

    fn delete(&self, resource: &Resource) -> Result<()> {
        self.terminate_resources(vec![resource.id.to_owned()].as_ref())
    }

    fn as_any(&self) -> &dyn::std::any::Any {
        self
    }
}
