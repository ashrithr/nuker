use {
    crate::{aws::AwsClient, config::Config, error::Error as AwsError},
    ansi_term::Color::{Cyan, Yellow},
    log::{error, info},
    rusoto_core::Region,
    std::str::FromStr,
};

type Result<T, E = AwsError> = std::result::Result<T, E>;

pub struct Nuke {
    config: Config,
}

impl Nuke {
    pub fn new(config: Config) -> Self {
        Nuke { config }
    }

    pub fn run(&self) -> Result<()> {
        let mut clients: Vec<AwsClient> = Vec::new();

        if self.config.dry_run {
            info!("{}", Cyan.paint("DRY RUN ENABLED"));
        } else {
            info!("{}", Yellow.paint("DRY RUN DISABLED"));
        }

        for region in &self.config.regions {
            let profile = &self.config.profile;
            let profile = profile.as_ref().map(|p| &**p);
            match AwsClient::new(profile, Region::from_str(region)?, &self.config) {
                Ok(client) => {
                    clients.push(client);
                }
                Err(err) => {
                    error!("initializing AWS Client for profile: {:?}.\n{}", profile, err);
                }
            }
        }

        for client in clients {
            info!("REGION: {}", Cyan.paint(client.region.name()));

            client.print_usage()?;
            let resources = client.locate_resources()?;
            client.cleanup_resources(&resources)?;
            // TODO:
            // client.print_savings(&resources)?;
        }

        Ok(())
    }
}
