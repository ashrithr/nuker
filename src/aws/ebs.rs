use crate::{
    aws::{cloudwatch::CwClient, Result},
    config::EbsConfig,
    service::{EnforcementState, NTag, NukeService, Resource, ResourceType},
};
use chrono::{DateTime, Utc};
use log::debug;
use rusoto_core::{credential::ProfileProvider, HttpClient, Region};
use rusoto_ec2::{
    DeleteSnapshotRequest, DeleteVolumeRequest, DescribeSnapshotsRequest, DescribeVolumesRequest,
    Ec2, Ec2Client, Filter, Snapshot, Tag, Volume,
};

pub struct EbsNukeClient {
    pub client: Ec2Client,
    pub cwclient: CwClient,
    pub config: EbsConfig,
    pub region: Region,
    pub dry_run: bool,
}

impl EbsNukeClient {
    pub fn new(
        profile_name: Option<&str>,
        region: Region,
        config: EbsConfig,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(EbsNukeClient {
                client: Ec2Client::new_with(HttpClient::new()?, pp, region.clone()),
                cwclient: CwClient::new(profile_name, region.clone(), config.clone().idle_rules)?,
                config,
                region,
                dry_run,
            })
        } else {
            Ok(EbsNukeClient {
                client: Ec2Client::new(region.clone()),
                cwclient: CwClient::new(profile_name, region.clone(), config.clone().idle_rules)?,
                config,
                region,
                dry_run,
            })
        }
    }

    fn package_volumes_as_resources(&self, volumes: Vec<Volume>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for volume in volumes {
            let volume_id = volume.volume_id.as_ref().unwrap().clone();

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&volume_id) {
                    debug!("Skipping resource from ignore list - {}", volume_id);
                    EnforcementState::SkipConfig
                } else if volume.volume_type != Some("gp2".to_string()) {
                    debug!("Resource is not a gp2 type volume - {}", volume_id);
                    EnforcementState::from_target_state(&self.config.target_state)
                } else if volume.state == Some("available".to_string()) {
                    debug!("Resource is idle (available) - {}", volume_id);
                    EnforcementState::from_target_state(&self.config.target_state)
                } else if self.is_resource_idle(&volume) {
                    // TODO: identify non-root volumes
                    debug!("Resource is idle - {}", volume_id);
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

    fn is_resource_idle(&self, volume: &Volume) -> bool {
        if !self.config.idle_rules.is_empty() {
            !self
                .cwclient
                .filter_volume(&volume.volume_id.as_ref().unwrap())
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

    fn get_volumes(&self) -> Result<Vec<Volume>> {
        let mut next_token: Option<String> = None;
        let mut volumes: Vec<Volume> = Vec::new();

        loop {
            let result = self
                .client
                .describe_volumes(DescribeVolumesRequest {
                    next_token,
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

    fn get_snapshots(&self) -> Result<Vec<Snapshot>> {
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
                .sync()?;

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

    fn delete_volume(&self, volume_id: String) -> Result<()> {
        debug!("Deleting Volume: {}", volume_id);

        if !self.dry_run {
            self.client
                .delete_volume(DeleteVolumeRequest {
                    volume_id: volume_id.to_owned(),
                    ..Default::default()
                })
                .sync()?;
        }

        Ok(())
    }

    fn delete_snapshot(&self, snapshot_id: String) -> Result<()> {
        debug!("Deleting snapshot: {}", snapshot_id);

        if !self.dry_run {
            self.client
                .delete_snapshot(DeleteSnapshotRequest {
                    snapshot_id,
                    ..Default::default()
                })
                .sync()?;
        }

        Ok(())
    }
}

impl NukeService for EbsNukeClient {
    fn scan(&self) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        let volumes = self.get_volumes()?;
        let snapshots: Vec<Snapshot> = self.get_snapshots()?;

        resources.append(&mut self.package_volumes_as_resources(volumes)?);
        resources.append(&mut self.package_snapshots_as_resources(snapshots)?);

        Ok(resources)
    }

    fn stop(&self, resource: &Resource) -> Result<()> {
        self.delete(resource)
    }

    fn delete(&self, resource: &Resource) -> Result<()> {
        if resource.resource_type.is_volume() {
            self.delete_volume(resource.id.to_owned())?;
        } else if resource.resource_type.is_snapshot() {
            self.delete_snapshot(resource.id.to_owned())?;
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
