use crate::client::Client;
use crate::{
    aws::AwsNuker,
    config::{Args, Config},
    Result,
};
use colored::*;
use rusoto_core::Region;
use std::{io, process::exit, str::FromStr};
use tracing::{debug, error, trace};
use tracing_futures::Instrument;

const PROMPT_YES: &str = "yes";

static REGIONS: &'static [Region] = &[
    Region::ApEast1,
    Region::ApNortheast1,
    Region::ApNortheast2,
    Region::ApSouth1,
    Region::ApSoutheast1,
    Region::ApSoutheast2,
    Region::CaCentral1,
    Region::EuCentral1,
    Region::EuNorth1,
    Region::EuWest1,
    Region::EuWest2,
    Region::EuWest3,
    Region::MeSouth1,
    Region::SaEast1,
    Region::UsEast1,
    Region::UsEast2,
    Region::UsWest1,
    Region::UsWest2,
];

pub struct Nuker {
    args: Args,
    config: Config,
}

impl Nuker {
    pub fn new(config: Config, args: Args) -> Self {
        trace!("Args: {:?}", args);
        Nuker { args, config }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut clients: Vec<AwsNuker> = Vec::new();
        let mut handles = Vec::new();

        if self.args.dry_run {
            println!("{}", "DRY RUN ENABLED".blue().bold());
        } else {
            println!("{}", "DRY RUN DISABLED".red().bold());
        }

        if !self.args.dry_run && !self.args.force {
            let input: String = self.get_input("Are you sure you want to continue (yes/no)?");
            if &input.to_lowercase() != PROMPT_YES {
                exit(1);
            }
        }

        let excluded_services = self.excluded_services();

        if self.args.regions.is_empty() {
            debug!("Scanning for resources across all regions - {:?}", REGIONS);

            for region in REGIONS.iter() {
                clients.push(
                    AwsNuker::new(
                        self.args.profile.clone(),
                        region.to_owned(),
                        self.config.clone(),
                        excluded_services.clone(),
                        self.args.dry_run,
                    )
                    .await?,
                );
            }
        } else {
            debug!("Scanning for resources in regions: {:?}", self.args.regions);

            for region in &self.args.regions {
                clients.push(
                    AwsNuker::new(
                        self.args.profile.clone(),
                        Region::from_str(region)?,
                        self.config.clone(),
                        excluded_services.clone(),
                        self.args.dry_run,
                    )
                    .await?,
                );
            }
        }

        for mut client in clients {
            handles.push(tokio::spawn(async move {
                let region = client.region.name().to_string();
                client
                    .locate_resources()
                    .instrument(tracing::trace_span!("nuker", region = region.as_str()))
                    .await;

                if let Err(err) = client.print_resources().await {
                    error!(err = ?err, "Failed printing resources");
                }

                if let Err(err) = client
                    .cleanup_resources()
                    .instrument(tracing::trace_span!("nuker", region = region.as_str()))
                    .await
                {
                    error!(err = ?err, "Failed cleaning up resources");
                }
            }));
        }

        trace!("Waiting for all futures to return");
        futures::future::join_all(handles).await;
        trace!("All futures completed");

        Ok(())
    }

    fn get_input(&self, prompt: &str) -> String {
        println!("{}", prompt);
        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {}
            Err(_) => {}
        }
        input.trim().to_string()
    }

    fn excluded_services(&mut self) -> Vec<Client> {
        if self.args.targets.is_some() {
            Client::iter()
                .filter(|s| !self.args.targets.as_ref().unwrap().contains(&s))
                .collect()
        } else if self.args.exclude.is_some() {
            self.args.exclude.as_ref().unwrap().clone()
        } else {
            vec![]
        }
    }
}
