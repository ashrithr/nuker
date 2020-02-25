use {
    crate::aws::Result,
    crate::config::EbsConfig,
    crate::service::{EnforcementState, NukeService, Resource, ResourceType},
    log::debug,
    rusoto_core::credential::ProfileProvider,
    rusoto_core::{HttpClient, Region},
    rusoto_ec2::{DeleteVolumeRequest, DescribeVolumesRequest, Ec2, Ec2Client, Filter, Volume},
};

pub struct EbsNukeClient {
    pub client: Ec2Client,
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
                config,
                region,
                dry_run,
            })
        } else {
            Ok(EbsNukeClient {
                client: Ec2Client::new(region.clone()),
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
                } else {
                    EnforcementState::Delete
                }
            };

            resources.push(Resource {
                id: volume_id,
                resource_type: ResourceType::Ec2Volume,
                region: self.region.clone(),
                tags: None,
                state: None,
                enforcement_state,
            });
        }

        Ok(resources)
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

    fn delete_volume(&self, volume_id: String) -> Result<()> {
        debug!("Deleting Volume: {:?}", volume_id);

        if !self.dry_run {
            self.client.delete_volume(DeleteVolumeRequest {
                volume_id: volume_id.to_owned(),
                ..Default::default()
            });
        }

        Ok(())
    }
}

impl NukeService for EbsNukeClient {
    fn scan(&self) -> Result<Vec<Resource>> {
        let volumes = self.get_volumes()?;

        Ok(self.package_volumes_as_resources(volumes)?)
    }

    fn stop(&self, resource: &Resource) -> Result<()> {
        self.delete_volume(resource.id.to_owned())
    }

    fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_volume(resource.id.to_owned())
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
