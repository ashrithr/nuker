use crate::{
    aws::AwsNuker,
    config::{Args, Config},
    error::Error as AwsError,
};
use colored::*;
use rusoto_core::Region;
use std::{io, process::exit, str::FromStr};
use tracing::{debug, trace};
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
            if &input.to_lowercase() != "yes" {
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
                Service::Aurora => self.config.aurora.enabled = false,
                Service::Ebs => self.config.ebs.enabled = false,
                Service::Ec2 => self.config.ec2.enabled = false,
                Service::Elb => self.config.elb.enabled = false,
                Service::Emr => self.config.emr.enabled = false,
                Service::Es => self.config.es.enabled = false,
                Service::Glue => self.config.glue.enabled = false,
                Service::Rds => self.config.rds.enabled = false,
                Service::Redshift => self.config.redshift.enabled = false,
                Service::S3 => self.config.s3.enabled = false,
                Service::Sagemaker => self.config.sagemaker.enabled = false,
                Service::Asg => self.config.asg.enabled = false,
            }
        }
    }
}
