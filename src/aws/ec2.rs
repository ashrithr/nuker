use {
    crate::aws::cloudwatch::CwClient,
    crate::config::Ec2Config,
    crate::config::TargetState,
    crate::error::Error as AwsError,
    crate::service::{NTag, NukeService, Resource, ResourceType},
    log::{debug, info, trace},
    rusoto_core::{HttpClient, Region},
    rusoto_credential::ProfileProvider,
    rusoto_ec2::{
        DescribeInstanceAttributeRequest, DescribeInstancesRequest, DescribeInstancesResult, Ec2,
        Ec2Client, Filter, Instance, ModifyInstanceAttributeRequest, StopInstancesRequest, Tag,
        TerminateInstancesRequest,
    },
};

type Result<T, E = AwsError> = std::result::Result<T, E>;

pub struct Ec2NukeClient {
    pub client: Ec2Client,
    pub cwclient: CwClient,
    pub config: Ec2Config,
    pub dry_run: bool,
}

impl Ec2NukeClient {
    pub fn new(
        profile_name: &String,
        region: Region,
        config: Ec2Config,
        dry_run: bool,
    ) -> Result<Self> {
        let mut pp = ProfileProvider::new()?;
        pp.set_profile(profile_name);

        Ok(Ec2NukeClient {
            client: Ec2Client::new_with(HttpClient::new()?, pp, region.clone()),
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

    fn package_instances_as_resources(
        &self,
        profile_name: &String,
        instances: Vec<&Instance>,
    ) -> Result<Vec<Resource>> {
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
                    profile_name: profile_name.to_owned(),
                    tags: self.package_tags_as_ntags(instance.tags.clone()),
                    state: instance.state.as_ref().unwrap().name.clone(),
                });
            }

            std::thread::sleep(std::time::Duration::from_millis(100));
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

            std::thread::sleep(std::time::Duration::from_millis(50));
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
                !self
                    .config
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

                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }

        if !self.dry_run {
            self.client
                .terminate_instances(TerminateInstancesRequest {
                    instance_ids: instance_ids.to_owned(),
                    ..Default::default()
                })
                .sync()?;
        }

        Ok(())
    }

    fn stop_instances(&self, instance_ids: &Vec<String>) -> Result<()> {
        debug!("Stopping instances: {:?}", instance_ids);

        if !self.dry_run {
            self.client
                .stop_instances(StopInstancesRequest {
                    instance_ids: instance_ids.to_owned(),
                    force: Some(true),
                    ..Default::default()
                })
                .sync()?;
        }

        Ok(())
    }
}

impl NukeService for Ec2NukeClient {
    fn scan(&self, profile_name: &String) -> Result<Vec<Resource>> {
        let instances = self.get_instances(Vec::new())?;
        let mut filtered_instances: Vec<&Instance> = Vec::new();

        let running_instances: Vec<&Instance> = instances
            .iter()
            .filter(|i| i.state.as_ref().unwrap().code == Some(16))
            .collect();
        let stopped_instances: Vec<&Instance> = instances
            .iter()
            .filter(|i| i.state.as_ref().unwrap().code == Some(80))
            .collect();

        let mut instances_filtered_by_tags = self.filter_by_tags(&running_instances);
        let mut instances_filtered_by_types = self.filter_by_types(&instances_filtered_by_tags);
        let mut idle_instances = self.filter_by_idle_rules(&instances_filtered_by_types);

        info!(
            "Instance Summary: \n\
             \tTotal Instances: {} \n\
             \tRunning Instances: {} \n\
             \tStopped Instances: {} \n\
             \tInstances that do not have required tags: {} \n\
             \tInstances that are not using the allowed instance-types: {} \n\
             \tInstances that are idle: {}",
            instances.len(),
            running_instances.len(),
            stopped_instances.len(),
            instances_filtered_by_tags.len(),
            instances_filtered_by_types.len(),
            idle_instances.len()
        );

        filtered_instances.append(&mut instances_filtered_by_tags);
        filtered_instances.append(&mut instances_filtered_by_types);
        filtered_instances.append(&mut idle_instances);

        Ok(self.package_instances_as_resources(profile_name, filtered_instances)?)
    }

    fn cleanup(&self, resources: Vec<&Resource>) -> Result<()> {
        let instance_ids = resources
            .into_iter()
            .map(|r| r.id.clone())
            .collect::<Vec<String>>();

        match self.config.target_state {
            TargetState::Stopped => Ok(self.stop_instances(&instance_ids)?),
            TargetState::Terminated => Ok(self.terminate_instances(&instance_ids)?),
        }
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
