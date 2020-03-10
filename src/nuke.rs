use crate::{
    aws::AwsNuker,
    config::{Args, Config},
    error::Error as AwsError,
};
use colored::*;
use rusoto_core::Region;
use std::io;
use std::process::exit;
use std::str::FromStr;
use tracing::debug;
use tracing_futures::Instrument;

type Result<T, E = AwsError> = std::result::Result<T, E>;

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
        Nuker { args, config }
    }

    pub async fn run(&self) -> Result<()> {
        let mut clients: Vec<AwsNuker> = Vec::new();
        let mut handles = Vec::new();

        if self.args.dry_run {
            println!("{}", "DRY RUN ENABLED".blue().bold());
        } else {
            println!("{}", "DRY RUN DISABLED".red().bold());
        }

        if !self.args.dry_run && !self.args.force {
            let input: String = self.get_input("Are you sure you want to continue (yes/no)?");
            if &input.to_lowercase() != "yes" {
                exit(1);
            }
        }

        if self.args.regions.is_empty() {
            debug!("Scanning for resources across all regions - {:?}", REGIONS);

            for region in REGIONS.iter() {
                clients.push(AwsNuker::new(
                    self.args.profile.clone(),
                    region.to_owned(),
                    &self.config,
                    self.args.dry_run,
                )?);
            }
        } else {
            debug!("Scanning for resources in regions: {:?}", self.args.regions);

            for region in &self.args.regions {
                clients.push(AwsNuker::new(
                    self.args.profile.clone(),
                    Region::from_str(region)?,
                    &self.config,
                    self.args.dry_run,
                )?);
            }
        }

        for mut client in clients {
            handles.push(tokio::spawn(async move {
                let region = client.region.name().to_string();
                client
                    .locate_resources()
                    .instrument(tracing::trace_span!("aws-nuker", region = region.as_str()))
                    .await;
                client.print_resources();
                let _ = client
                    .cleanup_resources()
                    .instrument(tracing::trace_span!("aws-nuker", region = region.as_str()))
                    .await;
            }));
        }

        futures::future::join_all(handles).await;

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
}
