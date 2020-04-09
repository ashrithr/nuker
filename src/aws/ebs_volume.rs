use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_ec2::{
    DeleteVolumeRequest, DescribeVolumesRequest, DetachVolumeRequest, Ec2, Ec2Client, Tag, Volume,
};
use std::str::FromStr;
use tracing::{debug, trace};

static ROOT_VOLUME_MOUNTS: &'static [&'static str] = &["/dev/sda1", "/dev/xvda"];
const GP2_TYPE: &str = "gp2";

#[derive(Clone)]
pub struct EbsVolumeClient {
    client: Ec2Client,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl EbsVolumeClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        EbsVolumeClient {
            client: Ec2Client::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn package_resources(&self, mut volumes: Vec<Volume>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for volume in &mut volumes {
            let vol_id = volume.volume_id.as_deref().unwrap();
            let is_root_vol = if let Some(ref attachments) = volume.attachments {
                attachments.iter().any(|at| {
                    ROOT_VOLUME_MOUNTS.contains(&at.device.as_deref().unwrap_or_default())
                })
            } else {
                false
            };

            if is_root_vol {
                debug!(resource = vol_id, "Skipping root volume.");
            }

            let arn = format!(
                "arn:aws:ec2:{}:{}:volume/{}",
                self.region.name(),
                self.account_num,
                volume.volume_id.as_ref().unwrap(),
            );

            resources.push(Resource {
                id: volume.volume_id.take().unwrap(),
                arn: Some(arn),
                type_: ClientType::EbsVolume,
                region: self.region.clone(),
                tags: self.package_tags(volume.tags.take()),
                state: ResourceState::from_str(volume.state.take().unwrap_or_default().as_str())
                    .ok(),
                start_time: volume.create_time.take(),
                enforcement_state: if is_root_vol {
                    EnforcementState::Skip
                } else {
                    EnforcementState::SkipUnknownState
                },
                resource_type: volume.volume_type.take().map(|t| vec![t]),
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

    async fn get_volumes(&self) -> Result<Vec<Volume>> {
        let mut next_token: Option<String> = None;
        let mut volumes: Vec<Volume> = Vec::new();

        loop {
            let req = self.client.describe_volumes(DescribeVolumesRequest {
                next_token,
                ..Default::default()
            });

            if let Ok(result) = handle_future_with_return!(req) {
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
            } else {
                break;
            }
        }

        Ok(volumes)
    }

    async fn detach_volume(&self, vol_id: &String) -> Result<()> {
        debug!("Detaching Volume: {}", vol_id);

        if !self.dry_run {
            let req = self.client.detach_volume(DetachVolumeRequest {
                volume_id: vol_id.to_string(),
                ..Default::default()
            });
            handle_future!(req);
        }

        Ok(())
    }

    async fn delete_volume(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting");

        if !self.dry_run {
            if resource.state == Some(ResourceState::Running) {
                self.detach_volume(&resource.id).await?;
            }

            let req = self.client.delete_volume(DeleteVolumeRequest {
                volume_id: resource.id.to_owned(),
                ..Default::default()
            });
            handle_future!(req);
        }

        Ok(())
    }
}

#[async_trait]
impl NukerClient for EbsVolumeClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized EBS Volume resource scanner");
        let volumes = self.get_volumes().await?;
        Ok(self.package_resources(volumes).await?)
    }

    async fn dependencies(&self, _resource: &Resource) -> Option<Vec<Resource>> {
        None
    }

    async fn additional_filters(
        &self,
        resource: &Resource,
        _config: &ResourceConfig,
    ) -> Option<bool> {
        if resource.resource_type == Some(vec![GP2_TYPE.to_string()]) {
            debug!(resource = resource.id.as_str(), "volume type violation.");
            Some(true)
        } else if resource.state == Some(ResourceState::Available) {
            debug!(resource = resource.id.as_str(), "volume state violation.");
            Some(true)
        } else {
            None
        }
    }

    async fn stop(&self, _resource: &Resource) -> Result<()> {
        Ok(())
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_volume(resource).await
    }
}
