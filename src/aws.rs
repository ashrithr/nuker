mod aurora;
mod ce;
mod cloudwatch;
mod ec2;
mod pricing;
mod rds;

use {
    crate::config::Config,
    crate::error::Error as AwsError,
    crate::service::{NukeService, Resource},
    aurora::AuroraNukeClient,
    ce::CeClient,
    ec2::Ec2NukeClient,
    log::{error, info},
    pricing::PriceClient,
    rds::RdsNukeClient,
    rusoto_core::Region,
};

type Result<T, E = AwsError> = std::result::Result<T, E>;

pub struct AwsClient {
    pub region: Region,
    clients: Vec<Box<dyn NukeService>>,
    pricing_client: PriceClient,
    ce_client: Option<CeClient>,
    profile_name: Option<String>,
}

impl AwsClient {
    pub fn new(
        profile_name: Option<&str>,
        region: Region,
        config: &Config,
    ) -> Result<AwsClient> {
        let mut clients: Vec<Box<dyn NukeService>> = Vec::new();

        if config.ec2.enabled {
            clients.push(Box::new(Ec2NukeClient::new(
                profile_name,
                region.clone(),
                config.ec2.clone(),
                config.dry_run,
            )?));
        }

        if config.rds.enabled {
            clients.push(Box::new(RdsNukeClient::new(
                profile_name,
                region.clone(),
                config.rds.clone(),
                config.dry_run,
            )?));
        }

        if config.aurora.enabled {
            clients.push(Box::new(AuroraNukeClient::new(
                profile_name,
                region.clone(),
                config.aurora.clone(),
                config.dry_run,
            )?))
        }

        let ce_client = if config.print_usage {
            Some(CeClient::new(profile_name, config.usage_days)?)
        } else {
            None
        };

        let pricing_client = PriceClient::new(profile_name)?;

        Ok(AwsClient {
            region,
            clients,
            pricing_client,
            ce_client,
            profile_name: profile_name.map(|s| s.into())
        })
    }

    pub fn locate_resources(&self) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for client in &self.clients {
            match client.scan() {
                Ok(rs) => {
                    if !rs.is_empty() {
                        for r in rs {
                            resources.push(r);
                        }
                    }
                }
                Err(err) => {
                    error!(
                        "Error occurred locating resources in profile: {:?}. {}",
                        self.profile_name, err
                    );
                }
            }
        }

        Ok(resources)
    }

    pub fn cleanup_resources(&self, resources: &Vec<Resource>) -> Result<()> {
        let ec2_resources: Vec<&Resource> = resources
            .iter()
            .filter(|r| r.resource_type.is_ec2())
            .collect();
        let rds_resources: Vec<&Resource> = resources
            .iter()
            .filter(|r| r.resource_type.is_rds())
            .collect();
        let aurora_resources: Vec<&Resource> = resources
            .iter()
            .filter(|r| r.resource_type.is_aurora())
            .collect();

        for client in &self.clients {
            let ref_client = client.as_any();

            if ref_client.is::<Ec2NukeClient>() {
                info!("Triggering cleanup of resources: {:?}", ec2_resources);
                client.cleanup(ec2_resources.to_owned())?;
            } else if ref_client.is::<RdsNukeClient>() {
                info!("Triggering cleanup of resources: {:?}", rds_resources);
                client.cleanup(rds_resources.to_owned())?;
            } else if ref_client.is::<AuroraNukeClient>() {
                info!("Triggering cleanup of resources: {:?}", aurora_resources);
                client.cleanup(aurora_resources.to_owned())?;
            }
        }

        Ok(())
    }

    pub fn print_savings(&self, _resources: &Vec<Resource>) -> Result<()> {
        self.pricing_client.get_ec2_pricing()?;

        Ok(())
    }

    pub fn print_usage(&self) -> Result<()> {
        if let Some(ce_client) = &self.ce_client {
            ce_client.get_usage()?;
        }

        Ok(())
    }
}
