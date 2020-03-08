use crate::{
    aws::AwsNuker,
    config::{Args, Config},
    error::Error as AwsError,
};
use colored::*;
use log::debug;
use rusoto_core::Region;
use std::io;
use std::process::exit;
use std::str::FromStr;

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
            for region in REGIONS.iter() {
                clients.push(
                    AwsNuker::new(
                        self.args.profile.clone(),
                        region.to_owned(),
                        &self.config,
                        self.args.dry_run,
                    )
                    .await?,
                );
            }
        } else {
            for region in &self.args.regions {
                clients.push(
                    AwsNuker::new(
                        self.args.profile.clone(),
                        Region::from_str(region)?,
                        &self.config,
                        self.args.dry_run,
                    )
                    .await?,
                );
            }
        }

        for mut client in clients {
            debug!("REGION: {}", client.region.name().blue().bold());

            handles.push(tokio::spawn(async move {
                client.locate_resources().await;
                client.print_resources();
                let _ = client.cleanup_resources().await;
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
