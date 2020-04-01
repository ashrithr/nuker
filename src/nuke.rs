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

        // Merge services enabled from Cli and Config
        self.enable_service_types();

        if self.args.regions.is_empty() {
            debug!("Scanning for resources across all regions - {:?}", REGIONS);

            for region in REGIONS.iter() {
                clients.push(AwsNuker::new(
                    self.args.profile.clone(),
                    region.to_owned(),
                    self.config.clone(),
                    self.args.dry_run,
                )?);
            }
        } else {
            debug!("Scanning for resources in regions: {:?}", self.args.regions);

            for region in &self.args.regions {
                clients.push(AwsNuker::new(
                    self.args.profile.clone(),
                    Region::from_str(region)?,
                    self.config.clone(),
                    self.args.dry_run,
                )?);
            }
        }

        for mut client in clients {
            handles.push(tokio::spawn(async move {
                let region = client.region.name().to_string();
                client
                    .locate_resources()
                    .instrument(tracing::trace_span!("nuker", region = region.as_str()))
                    .await;

                client.print_resources();

                if let Err(err) = client
                    .cleanup_resources()
                    .instrument(tracing::trace_span!("nuker", region = region.as_str()))
                    .await
                {
                    error!("Failed cleaning up resources: {:?}", err);
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

    fn enable_service_types(&mut self) {
        use crate::service::Service;

        let excludes: Vec<Service> = if self.args.targets.is_some() {
            Service::iter()
                .filter(|s| !self.args.targets.as_ref().unwrap().contains(&s))
                .collect()
        } else if self.args.exclude.is_some() {
            self.args.exclude.as_ref().unwrap().clone()
        } else {
            vec![]
        };

        trace!(services = ?excludes, "Excluding services");

        for exclude in excludes {
            match exclude {
                Service::Aurora => self.config.aurora = None,
                Service::Ebs => self.config.ebs = None,
                Service::Ec2 => self.config.ec2 = None,
                Service::Elb => self.config.elb = None,
                Service::Emr => self.config.emr = None,
                Service::Es => self.config.es = None,
                Service::Glue => self.config.glue = None,
                Service::Rds => self.config.rds = None,
                Service::Redshift => self.config.redshift = None,
                Service::S3 => self.config.s3 = None,
                Service::Sagemaker => self.config.sagemaker = None,
                Service::Asg => self.config.asg = None,
                Service::Ecs => self.config.ecs = None,
                Service::Vpc => self.config.vpc = None,
            }
        }
    }
}
