use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_rds::{
    DBInstance, DeleteDBInstanceMessage, DescribeDBInstancesMessage, Filter,
    ListTagsForResourceMessage, ModifyDBInstanceMessage, Rds, RdsClient, StopDBInstanceMessage,
    Tag,
};
use std::str::FromStr;
use tracing::{debug, trace};

const AURORA_POSTGRES_ENGINE: &str = "aurora-postgresql";
const AURORA_MYSQL_ENGINE: &str = "aurora-mysql";

#[derive(Clone)]
pub struct RdsInstanceClient {
    client: RdsClient,
    config: ResourceConfig,
    account_num: String,
    region: Region,
    dry_run: bool,
}

impl RdsInstanceClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        RdsInstanceClient {
            client: RdsClient::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    fn package_tags(&self, tags: Option<Vec<Tag>>) -> Option<Vec<NTag>> {
        tags.map(|ts| {
            ts.into_iter()
                .map(|mut tag| NTag {
                    key: std::mem::replace(&mut tag.key, None),
                    value: std::mem::replace(&mut tag.value, None),
                })
                .collect()
        })
    }

    async fn package_resources(&self, mut instances: Vec<DBInstance>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for instance in &mut instances {
            let mut termination_protection: Option<bool> = None;

            if let Some(ref tp) = self.config.termination_protection {
                if tp.ignore {
                    termination_protection = instance.deletion_protection;
                }
            }

            resources.push(Resource {
                id: instance.db_instance_identifier.take().unwrap(),
                arn: instance.db_instance_arn.take(),
                type_: ClientType::RdsInstance,
                region: self.region.clone(),
                resource_type: instance.db_instance_class.take().map(|t| vec![t]),
                tags: self.package_tags(
                    self.list_tags(instance.db_instance_arn.as_ref().unwrap())
                        .await,
                ),
                state: Some(
                    ResourceState::from_str(instance.db_instance_status.as_ref().unwrap()).unwrap(),
                ),
                start_time: instance.instance_create_time.take(),
                enforcement_state: EnforcementState::SkipUnknownState,
                dependencies: None,
                termination_protection,
            });
        }

        Ok(resources)
    }

    async fn get_instances(&self, filter: Vec<Filter>) -> Result<Vec<DBInstance>> {
        let mut next_token: Option<String> = None;
        let mut instances: Vec<DBInstance> = Vec::new();

        loop {
            let req = self
                .client
                .describe_db_instances(DescribeDBInstancesMessage {
                    filters: Some(filter.clone()),
                    marker: next_token,
                    ..Default::default()
                });

            if let Ok(result) = handle_future_with_return!(req) {
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
            } else {
                break;
            }
        }

        Ok(instances)
    }

    async fn list_tags(&self, arn: &String) -> Option<Vec<Tag>> {
        let req = self
            .client
            .list_tags_for_resource(ListTagsForResourceMessage {
                resource_name: arn.to_owned(),
                ..Default::default()
            });

        handle_future_with_return!(req)
            .ok()
            .map(|r| r.tag_list)
            .unwrap_or_default()
    }

    async fn disable_termination_protection(&self, instance_id: &str) -> Result<()> {
        debug!(
            "Termination protection is enabled for: {}. Trying to disable it.",
            instance_id
        );

        if !self.dry_run {
            let req = self.client.modify_db_instance(ModifyDBInstanceMessage {
                db_instance_identifier: instance_id.to_owned(),
                deletion_protection: Some(false),
                ..Default::default()
            });

            handle_future!(req);
        }

        Ok(())
    }

    async fn delete_instance(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting");

        if !self.dry_run {
            if let Some(tp_enabled) = resource.termination_protection {
                if tp_enabled {
                    self.disable_termination_protection(resource.id.as_ref())
                        .await?;
                }
            }

            let req = self.client.delete_db_instance(DeleteDBInstanceMessage {
                db_instance_identifier: resource.id.to_owned(),
                delete_automated_backups: Some(false),
                skip_final_snapshot: Some(true),
                ..Default::default()
            });

            handle_future!(req);
        }

        Ok(())
    }

    async fn stop_instance(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Stopping");

        if !self.dry_run {
            let req = self.client.stop_db_instance(StopDBInstanceMessage {
                db_instance_identifier: resource.id.to_owned(),
                ..Default::default()
            });

            handle_future!(req);
        }

        Ok(())
    }
}

#[async_trait]
impl NukerClient for RdsInstanceClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized RDS resource scanner");
        let instances = self.get_instances(Vec::new()).await?;

        Ok(self.package_resources(instances).await?)
    }

    async fn dependencies(&self, _resource: &Resource) -> Option<Vec<Resource>> {
        None
    }

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.stop_instance(resource).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_instance(resource).await
    }

    async fn additional_filters(
        &self,
        _resource: &Resource,
        _config: &ResourceConfig,
    ) -> Option<bool> {
        None
    }
}
