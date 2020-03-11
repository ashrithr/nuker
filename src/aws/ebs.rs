use crate::{
    aws::{cloudwatch::CwClient, Result},
    config::EbsConfig,
    resource::{EnforcementState, NTag, Resource, ResourceType},
    service::NukerService,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusoto_core::{credential::ProfileProvider, HttpClient, Region};
use rusoto_ec2::{
    DeleteSnapshotRequest, DeleteVolumeRequest, DescribeSnapshotsRequest, DescribeVolumesRequest,
    DetachVolumeRequest, Ec2, Ec2Client, Filter, Snapshot, Tag, Volume,
};
use tracing::{debug, trace};

#[derive(Clone)]
pub struct EbsService {
    pub client: Ec2Client,
    pub cw_client: CwClient,
    pub config: EbsConfig,
    pub region: Region,
    pub dry_run: bool,
}

impl EbsService {
    pub fn new(
        profile_name: Option<String>,
        region: Region,
        config: EbsConfig,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = &profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(EbsService {
                client: Ec2Client::new_with(HttpClient::new()?, pp, region.clone()),
                cw_client: CwClient::new(
                    profile_name.clone(),
                    region.clone(),
                    config.clone().idle_rules,
                )?,
                config,
                region,
                dry_run,
            })
        } else {
            Ok(EbsService {
                client: Ec2Client::new(region.clone()),
                cw_client: CwClient::new(
                    profile_name.clone(),
                    region.clone(),
                    config.clone().idle_rules,
                )?,
                config,
                region,
                dry_run,
            })
        }
    }

    async fn package_volumes_as_resources(&self, volumes: Vec<Volume>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for volume in volumes {
            let volume_id = volume.volume_id.as_ref().unwrap().clone();

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&volume_id) {
                    debug!(
                        resource = volume_id.as_str(),
                        "Skipping resource from ignore list"
                    );
                    EnforcementState::SkipConfig
                } else if volume.volume_type != Some("gp2".to_string()) {
                    debug!(
                        resource = volume_id.as_str(),
                        "Resource is not a gp2 type volume"
                    );
                    EnforcementState::from_target_state(&self.config.target_state)
                } else if volume.state == Some("available".to_string()) {
                    debug!(
                        resource = volume_id.as_str(),
                        "Resource is idle (available)"
                    );
                    EnforcementState::from_target_state(&self.config.target_state)
                } else if self.is_resource_idle(&volume).await && !self.is_root_volume(&volume) {
                    debug!(resource = volume_id.as_str(), "Resource is idle");
                    EnforcementState::from_target_state(&self.config.target_state)
                } else {
                    EnforcementState::Skip
                }
            };

            resources.push(Resource {
                id: volume_id,
                resource_type: ResourceType::EbsVolume,
                region: self.region.clone(),
                tags: self.package_tags_as_ntags(volume.tags.clone()),
                state: volume.state,
                enforcement_state,
            });
        }

        Ok(resources)
    }

    fn package_snapshots_as_resources(&self, snapshots: Vec<Snapshot>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for snapshot in snapshots {
            let snap_id = snapshot.snapshot_id.as_ref().unwrap().clone();

            let enforcement_state: EnforcementState = {
                if self.is_snapshot_old(&snapshot) {
                    debug!("Resource is old - {}", snap_id);
                    EnforcementState::from_target_state(&self.config.target_state)
                } else {
                    EnforcementState::Skip
                }
            };

            resources.push(Resource {
                id: snap_id,
                resource_type: ResourceType::EbsSnapshot,
                region: self.region.clone(),
                tags: self.package_tags_as_ntags(snapshot.tags.clone()),
                state: snapshot.state,
                enforcement_state,
            });
        }

        Ok(resources)
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

    async fn is_resource_idle(&self, volume: &Volume) -> bool {
        if !self.config.idle_rules.is_empty() {
            !self
                .cw_client
                .filter_volume(&volume.volume_id.as_ref().unwrap())
                .await
                .unwrap()
        } else {
            false
        }
    }

    fn is_snapshot_old(&self, snapshot: &Snapshot) -> bool {
        if self.config.older_than.is_some() && snapshot.start_time.is_some() {
            match DateTime::parse_from_rfc3339(snapshot.start_time.as_ref().unwrap()) {
                Ok(ts) => {
                    let start = Utc::now().timestamp_millis()
                        - self.config.older_than.unwrap().as_millis() as i64;
                    if start > ts.timestamp_millis() {
                        true
                    } else {
                        false
                    }
                }
                Err(_e) => false,
            }
        } else {
            false
        }
    }

    fn is_root_volume(&self, volume: &Volume) -> bool {
        let root_attachments = vec!["/dev/sda1", "/dev/xvda"];
        if let Some(ref attachments) = volume.attachments {
            attachments.iter().any(|vol_att| {
                if let Some(ref device) = vol_att.device {
                    root_attachments.contains(&device.as_str())
                } else {
                    false
                }
            })
        } else {
            false
        }
    }

    async fn get_volumes(&self) -> Result<Vec<Volume>> {
        let mut next_token: Option<String> = None;
        let mut volumes: Vec<Volume> = Vec::new();

        loop {
            let result = self
                .client
                .describe_volumes(DescribeVolumesRequest {
                    next_token,
                    ..Default::default()
                })
                .await?;

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

    async fn get_snapshots(&self) -> Result<Vec<Snapshot>> {
        let mut next_token: Option<String> = None;
        let mut snapshots: Vec<Snapshot> = Vec::new();

        loop {
            let result = self
                .client
                .describe_snapshots(DescribeSnapshotsRequest {
                    next_token,
                    filters: Some(vec![Filter {
                        name: Some("owner-alias".to_string()),
                        values: Some(vec!["self".to_string()]),
                    }]),
                    ..Default::default()
                })
                .await?;

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
        }

        Ok(snapshots)
    }

    async fn detach_volume(&self, volume_id: &String) -> Result<()> {
        debug!("Detaching Volume: {}", volume_id);

        if !self.dry_run {
            self.client
                .detach_volume(DetachVolumeRequest {
                    volume_id: volume_id.to_string(),
                    ..Default::default()
                })
                .await?;
        }

        Ok(())
    }

    async fn delete_volume(&self, volume_id: String) -> Result<()> {
        debug!("Deleting Volume: {}", volume_id);

        if !self.dry_run {
            self.detach_volume(&volume_id).await?;

            self.client
                .delete_volume(DeleteVolumeRequest {
                    volume_id: volume_id.to_owned(),
                    ..Default::default()
                })
                .await?;
        }

        Ok(())
    }

    async fn delete_snapshot(&self, snapshot_id: String) -> Result<()> {
        debug!("Deleting snapshot: {}", snapshot_id);

        if !self.dry_run {
            self.client
                .delete_snapshot(DeleteSnapshotRequest {
                    snapshot_id,
                    ..Default::default()
                })
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl NukerService for EbsService {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized EBS resource scanner");
        let mut resources: Vec<Resource> = Vec::new();

        let volumes = self.get_volumes().await?;
        let snapshots: Vec<Snapshot> = self.get_snapshots().await?;

        resources.append(&mut self.package_volumes_as_resources(volumes).await?);
        resources.append(&mut self.package_snapshots_as_resources(snapshots)?);

        Ok(resources)
    }

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.delete(resource).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        if resource.resource_type.is_volume() {
            self.delete_volume(resource.id.to_owned()).await?;
        } else if resource.resource_type.is_snapshot() {
            self.delete_snapshot(resource.id.to_owned()).await?;
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
