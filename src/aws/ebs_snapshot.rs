use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_ec2::{
    DeleteSnapshotRequest, DescribeSnapshotsRequest, Ec2, Ec2Client, Filter, Snapshot, Tag,
};
use std::str::FromStr;
use tracing::{debug, trace};

#[derive(Clone)]
pub struct EbsSnapshotClient {
    client: Ec2Client,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl EbsSnapshotClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        EbsSnapshotClient {
            client: Ec2Client::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn package_resources(&self, mut snapshots: Vec<Snapshot>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for snapshot in &mut snapshots {
            let arn = format!(
                "arn:aws:ec2:{}:{}:snapshot/{}",
                self.region.name(),
                self.account_num,
                snapshot.snapshot_id.as_ref().unwrap(),
            );

            resources.push(Resource {
                id: snapshot.snapshot_id.take().unwrap(),
                arn: Some(arn),
                type_: ClientType::EbsSnapshot,
                region: self.region.clone(),
                tags: self.package_tags(snapshot.tags.take()),
                state: ResourceState::from_str(snapshot.state.take().unwrap_or_default().as_str())
                    .ok(),
                start_time: snapshot.start_time.take(),
                enforcement_state: EnforcementState::SkipUnknownState,
                enforcement_reason: None,
                resource_type: None,
                dependencies: None,
                termination_protection: None,
            });
        }

        Ok(resources)
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

    async fn get_snapshots(&self) -> Result<Vec<Snapshot>> {
        let mut next_token: Option<String> = None;
        let mut snapshots: Vec<Snapshot> = Vec::new();

        loop {
            let req = self.client.describe_snapshots(DescribeSnapshotsRequest {
                next_token,
                filters: Some(vec![Filter {
                    name: Some("owner-alias".to_string()),
                    values: Some(vec!["self".to_string()]),
                }]),
                ..Default::default()
            });

            if let Ok(result) = handle_future_with_return!(req) {
                if let Some(snaps) = result.snapshots {
                    for s in snaps {
                        snapshots.push(s);
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

        Ok(snapshots)
    }

    async fn delete_snapshot(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting");

        if !self.dry_run {
            let req = self.client.delete_snapshot(DeleteSnapshotRequest {
                snapshot_id: resource.id.to_owned(),
                ..Default::default()
            });
            handle_future!(req);
        }

        Ok(())
    }
}

#[async_trait]
impl NukerClient for EbsSnapshotClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized EBS Snapshot resource scanner");
        let snapshots = self.get_snapshots().await?;
        Ok(self.package_resources(snapshots).await?)
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
        self.delete_snapshot(resource).await
    }
}
