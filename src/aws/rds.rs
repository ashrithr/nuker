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
        DBInstance, DeleteDBInstanceMessage, DescribeDBInstancesMessage, Filter,
        ListTagsForResourceMessage, ModifyDBInstanceMessage, Rds, RdsClient, StopDBInstanceMessage,
        Tag,
    },
};

type Result<T, E = AwsError> = std::result::Result<T, E>;
const AURORA_POSTGRES_ENGINE: &str = "aurora-postgresql";
const AURORA_MYSQL_ENGINE: &str = "aurora-mysql";

pub struct RdsNukeClient {
    pub client: RdsClient,
    pub cwclient: CwClient,
    pub config: RdsConfig,
    pub dry_run: bool,
}

impl RdsNukeClient {
    pub fn new(
        profile_name: &String,
        region: Region,
        config: RdsConfig,
        dry_run: bool,
    ) -> Result<Self> {
        let mut pp = ProfileProvider::new()?;
        pp.set_profile(profile_name);

        Ok(RdsNukeClient {
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

    fn package_instances_as_resources(
        &self,
        profile_name: &String,
        instances: Vec<&DBInstance>,
    ) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for instance in instances {
            let instance_id = instance.db_instance_identifier.as_ref().unwrap().to_owned();

            if let Some(_resource) = resources.iter().find(|r| r.id == instance_id) {
                trace!("Skipping resource, already exists in the list.");
            } else {
                resources.push(Resource {
                    id: instance_id,
                    resource_type: ResourceType::RDS,
                    profile_name: profile_name.to_owned(),
                    tags: self
                        .package_tags_as_ntags(self.list_tags(instance.db_instance_arn.clone())?),
                    state: instance.db_instance_status.clone(),
                });
            }

            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        Ok(resources)
    }

    fn get_instances(&self, filter: Vec<Filter>) -> Result<Vec<DBInstance>> {
        let mut next_token: Option<String> = None;
        let mut instances: Vec<DBInstance> = Vec::new();

        loop {
            let result = self
                .client
                .describe_db_instances(DescribeDBInstancesMessage {
                    filters: Some(filter.clone()),
                    marker: next_token,
                    ..Default::default()
                })
                .sync()?;

            if let Some(db_instances) = result.db_instances {
                let mut temp_instances: Vec<DBInstance> = db_instances
                    .into_iter()
                    .filter(|i| {
                        i.engine != Some(AURORA_MYSQL_ENGINE.into())
                            && i.engine != Some(AURORA_POSTGRES_ENGINE.into())
                    })
                    .collect();

                instances.append(&mut temp_instances);
            }

            if result.marker.is_none() {
                break;
            } else {
                next_token = result.marker;
            }

            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        trace!("RDS get_instances: {:?}", instances);

        Ok(instances)
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

    fn filter_by_tags<'a>(&self, instances: &Vec<&'a DBInstance>) -> Vec<&'a DBInstance> {
        debug!(
            "Total # of db instances before applying Filter by required tags - {:?}: {}.",
            &self.config.required_tags,
            instances.len()
        );

        instances
            .iter()
            .filter(|instance| {
                !self.check_tags(
                    &self
                        .list_tags(instance.db_instance_arn.clone())
                        .unwrap_or_default(),
                    &self.config.required_tags,
                )
            })
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

    fn filter_by_types<'a>(&self, instances: &Vec<&'a DBInstance>) -> Vec<&'a DBInstance> {
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
                    .any(|it| Some(it) == instance.db_instance_class.as_ref())
            })
            .cloned()
            .collect()
    }

    fn filter_by_idle_rules<'a>(&self, instances: &Vec<&'a DBInstance>) -> Vec<&'a DBInstance> {
        debug!(
            "Total # of instances before applying Filter by CPU Utilization - {:?}: {}",
            self.config.idle_rules,
            instances.len()
        );

        instances
            .iter()
            .filter(|instance| {
                instance.db_instance_status == Some("available".to_string())
                    && !self
                        .cwclient
                        .filter_db_instance_by_utilization(
                            &instance.db_instance_identifier.as_ref().unwrap(),
                        )
                        .unwrap()
                    && !self
                        .cwclient
                        .filter_db_instance_by_connections(
                            &instance.db_instance_identifier.as_ref().unwrap(),
                        )
                        .unwrap()
            })
            .cloned()
            .collect()
    }

    fn disable_termination_protection(&self, instance_id: &str) -> Result<()> {
        let resp = self
            .client
            .describe_db_instances(DescribeDBInstancesMessage {
                db_instance_identifier: Some(instance_id.to_owned()),
                ..Default::default()
            })
            .sync()?;

        if resp.db_instances.is_some() {
            if resp
                .db_instances
                .unwrap()
                .first()
                .unwrap()
                .deletion_protection
                == Some(true)
            {
                debug!(
                    "Terminating protection was enabled for: {}. Trying to disable it.",
                    instance_id
                );

                if !self.dry_run {
                    self.client
                        .modify_db_instance(ModifyDBInstanceMessage {
                            db_instance_identifier: instance_id.to_owned(),
                            deletion_protection: Some(false),
                            ..Default::default()
                        })
                        .sync()?;
                }
            }
        }

        Ok(())
    }

    fn terminate_instances(&self, instance_ids: &Vec<String>) -> Result<()> {
        debug!("Terminating instances: {:?}", instance_ids);

        if self.config.termination_protection.ignore {
            for instance_id in instance_ids {
                self.disable_termination_protection(instance_id)?;
            }
        }

        if !self.dry_run {
            for instance_id in instance_ids {
                self.client
                    .delete_db_instance(DeleteDBInstanceMessage {
                        db_instance_identifier: instance_id.to_owned(),
                        delete_automated_backups: Some(false),
                        ..Default::default()
                    })
                    .sync()?;

                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }

        Ok(())
    }

    fn stop_instances(&self, instance_ids: &Vec<String>) -> Result<()> {
        debug!("Stopping instances: {:?}", instance_ids);

        if !self.dry_run {
            for instance_id in instance_ids {
                self.client
                    .stop_db_instance(StopDBInstanceMessage {
                        db_instance_identifier: instance_id.to_owned(),
                        ..Default::default()
                    })
                    .sync()?;

                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }

        Ok(())
    }
}

impl NukeService for RdsNukeClient {
    fn scan(&self, profile_name: &String) -> Result<Vec<Resource>> {
        let instances = self.get_instances(Vec::new())?;
        let mut filtered_instances: Vec<&DBInstance> = Vec::new();

        // https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Status.html
        let running_instances: Vec<&DBInstance> = instances
            .iter()
            .filter(|i| i.db_instance_status == Some("available".to_string()))
            .collect();
        let stopped_instances: Vec<&DBInstance> = instances
            .iter()
            .filter(|i| i.db_instance_status == Some("stopped".to_string()))
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
