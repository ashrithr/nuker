//! Configuration Parser
use crate::client::Client;
use clap::{App, Arg};
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::{fs::File, io::Read, str::FromStr, time::Duration};
use tracing::warn;

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

pub type Config = HashMap<Client, ResourceConfig>;

/// Cli Args
#[derive(Debug, Clone)]
pub struct Args {
    pub config: String,
    pub profile: Option<String>,
    pub regions: Vec<String>,
    pub targets: Option<Vec<Client>>,
    pub exclude: Option<Vec<Client>>,
    pub dry_run: bool,
    pub force: bool,
    pub verbose: u64,
    pub version: String,
}

/// Configuration struct for nuker executable
///
/// This struct is built from reading the configuration file
#[derive(Debug, Deserialize, Clone)]
pub struct ParsedConfig {
    #[serde(default = "default_resource_config")]
    pub ec2_instance: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub ec2_sg: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub ec2_eni: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub ec2_address: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub ebs_volume: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub ebs_snapshot: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub elb_alb: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub elb_nlb: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub rds_instance: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub rds_cluster: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub s3: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub emr_cluster: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub redshift: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub glue: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub sagemaker: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub es_domain: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub asg: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub ecs: ResourceConfig,
    #[serde(default = "default_resource_config")]
    pub vpc: ResourceConfig,
}

#[derive(Debug, Deserialize, Clone, Eq, PartialEq)]
pub enum TargetState {
    Stopped,
    Deleted,
}

impl Default for TargetState {
    fn default() -> Self {
        TargetState::Deleted
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct RequiredTags {
    pub name: String,
    pub pattern: Option<String>,
    #[serde(skip)]
    pub regex: Option<Regex>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct IdleRules {
    pub namespace: String,
    pub metric: String,
    pub minimum: Option<f32>,
    #[serde(with = "humantime_serde")]
    pub duration: Duration,
    #[serde(with = "humantime_serde")]
    pub granularity: Duration,
    pub connections: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TerminationProtection {
    pub ignore: bool,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ManageStopped {
    #[serde(with = "humantime_serde")]
    pub older_than: Duration,
    #[serde(skip)]
    pub dt_extract_regex: Option<Regex>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct SecurityGroups {
    pub source_cidr: Vec<String>,
    pub from_port: u16,
    pub to_port: u16,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct Eni {
    pub cleanup: bool,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct Eip {
    pub cleanup: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ResourceConfig {
    #[serde(default)]
    pub target_state: TargetState,
    #[serde(default)]
    pub required_tags: Option<Vec<RequiredTags>>,
    #[serde(default)]
    pub allowed_types: Option<Vec<String>>,
    #[serde(default)]
    pub whitelist: Option<Vec<String>>,
    #[serde(default)]
    pub idle_rules: Option<Vec<IdleRules>>,
    #[serde(default)]
    pub termination_protection: Option<TerminationProtection>,
    #[serde(default)]
    pub manage_stopped: Option<ManageStopped>,
    #[serde(default)]
    #[serde(with = "humantime_serde")]
    pub max_run_time: Option<Duration>,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        ResourceConfig {
            target_state: TargetState::Deleted,
            required_tags: None,
            allowed_types: None,
            whitelist: None,
            idle_rules: None,
            termination_protection: Some(TerminationProtection { ignore: true }),
            manage_stopped: None,
            max_run_time: None,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Ec2Config {
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    pub idle_rules: Option<Vec<IdleRules>>,
    pub termination_protection: TerminationProtection,
    pub manage_stopped: Option<ManageStopped>,
    pub security_groups: SecurityGroups,
    pub eni: Eni,
    pub eip: Eip,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EbsConfig {
    pub target_state: TargetState,
    pub ignore: Vec<String>,
    pub idle_rules: Option<Vec<IdleRules>>,
    #[serde(with = "humantime_serde")]
    pub older_than: Option<Duration>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct Igw {
    pub cleanup: bool,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct Vgw {
    pub cleanup: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct VpcConfig {
    pub target_state: TargetState,
    pub ignore: Vec<String>,
    pub cleanup_empty: bool,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub igw: Igw,
    pub vgw: Vgw,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EmrConfig {
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    pub idle_rules: Option<Vec<IdleRules>>,
    pub termination_protection: TerminationProtection,
    pub security_groups: SecurityGroups,
    #[serde(with = "humantime_serde")]
    pub older_than: Duration,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RdsConfig {
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub allowed_instance_types: Vec<String>,
    pub manage_stopped: ManageStopped,
    pub ignore: Vec<String>,
    pub idle_rules: Option<Vec<IdleRules>>,
    pub termination_protection: TerminationProtection,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuroraConfig {
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    pub idle_rules: Option<Vec<IdleRules>>,
    pub termination_protection: TerminationProtection,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RedshiftConfig {
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    pub idle_rules: Option<Vec<IdleRules>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct S3Config {
    pub target_state: TargetState,
    pub check_dns_compliant_naming: Option<bool>,
    pub required_naming_prefix: Option<String>,
    #[serde(skip)]
    pub required_naming_regex: Option<Regex>,
    pub check_public_accessibility: Option<bool>,
    pub ignore: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GlueConfig {
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub ignore: Vec<String>,
    #[serde(with = "humantime_serde")]
    pub older_than: Duration,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SagemakerConfig {
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    #[serde(with = "humantime_serde")]
    pub older_than: Duration,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EsConfig {
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    pub idle_rules: Option<Vec<IdleRules>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ElbConfig {
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub ignore: Vec<String>,
    pub alb_idle_rules: Option<Vec<IdleRules>>,
    pub nlb_idle_rules: Option<Vec<IdleRules>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AutoScalingConfig {
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub ignore: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EcsConfig {
    pub target_state: TargetState,
    pub allowed_instance_types: Vec<String>,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub ignore: Vec<String>,
    pub idle_rules: Option<Vec<IdleRules>>,
}

/// Parse the command line arguments for nuker executable
pub fn parse_args() -> Args {
    let args = App::new("nuker")
        .about("Cleans up AWS resources based on configurable Rules.")
        .version(VERSION.unwrap_or("unknown"))
        .subcommand(App::new("resource-types").about("Prints out supported resource types"))
        .arg(
            Arg::with_name("config-file")
                .long("config")
                .short("C")
                .value_name("config")
                // .required(true)
                .help("The config file to feed in.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("region")
                .long("region")
                .help(
                    "Which regions to enforce the rules in. Default is the rules will be \
                    enforced across all the regions.",
                )
                .takes_value(true)
                .multiple(true)
                .number_of_values(1),
        )
        .arg(
            Arg::with_name("target")
                .long("target")
                .help(
                    "Services to include from rules enforcement. This will take precedence \
                over the configuration file.",
                )
                .takes_value(true)
                .multiple(true)
                .number_of_values(1),
        )
        .arg(
            Arg::with_name("exclude")
                .long("exclude")
                .help(
                    "Services to exclude from rules enforcement. This will take precedence \
                over the configuration file.",
                )
                .takes_value(true)
                .multiple(true)
                .number_of_values(1)
                .conflicts_with("target"),
        )
        .arg(
            Arg::with_name("profile")
                .long("profile")
                .help(
                    "Named Profile to use for authenticating with AWS. If the profile is \
                    *not* set, the credentials will be sourced in the following order: \n\
                    1. Environment variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY \n\
                    2. AWS credentials file. Usually located at ~/.aws/credentials.\n\
                    3. IAM instance profile",
                )
                .takes_value(true),
        )
        .arg(Arg::with_name("no-dry-run").long("no-dry-run").help(
            "Disables the dry run behavior, which just lists the resources that are \
                    being cleaned but not actually delete them. Enabling this option will disable \
                    dry run behavior and deletes the resources.",
        ))
        .arg(
            Arg::with_name("force")
                .long("force")
                .help("Does not prompt for confirmation when dry run is disabled"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .multiple(true)
                .help("Turn on verbose output."),
        )
        .get_matches();

    if let Some(ref _matches) = args.subcommand_matches("resource-types") {
        for r in Client::iter() {
            print!("{} ", r.name());
        }
        ::std::process::exit(0);
    }

    if !args.is_present("config-file") {
        panic!("--config <config> is a required parameter");
    }

    let verbose = if args.is_present("verbose") {
        args.occurrences_of("verbose")
    } else {
        0
    };

    let dry_run = if args.is_present("no-dry-run") {
        false
    } else {
        true
    };

    let force = if args.is_present("force") {
        true
    } else {
        false
    };

    let regions: Vec<&str> = if args.is_present("region") {
        args.values_of("region").unwrap().collect()
    } else {
        vec![]
    };

    let targets: Option<Vec<Client>> = if args.is_present("target") {
        Some(
            args.values_of("target")
                .unwrap()
                .map(|t| Client::from_str(t).unwrap())
                .collect(),
        )
    } else {
        None
    };

    let exclude: Option<Vec<Client>> = if args.is_present("exclude") {
        Some(
            args.values_of("exclude")
                .unwrap()
                .map(|e| Client::from_str(e).unwrap())
                .collect(),
        )
    } else {
        None
    };

    Args {
        config: args.value_of("config-file").unwrap().to_string(),
        regions: regions.iter().map(|r| r.to_string()).collect(),
        profile: args.value_of("profile").map(|s| s.to_owned()),
        targets,
        exclude,
        dry_run,
        force,
        verbose,
        version: VERSION.unwrap_or("unknown").to_string(),
    }
}

/// Parses the nuker configuration file
pub fn parse_config_file(filename: &str) -> Config {
    let mut fp = match File::open(filename) {
        Err(e) => panic!("Could not open file {} with error {}", filename, e),
        Ok(fp) => fp,
    };

    let mut buffer = String::new();
    fp.read_to_string(&mut buffer).unwrap();
    parse_config(&buffer)
}

pub fn parse_config(buffer: &str) -> Config {
    let mut config: ParsedConfig =
        toml::from_str(buffer).expect("could not parse toml configuration");
    let mut config_map: HashMap<Client, ResourceConfig> = HashMap::new();

    // Compile all regex expressions up front
    if config.ec2_instance.required_tags.is_some() {
        for rt in config.ec2_instance.required_tags.as_mut().unwrap() {
            if rt.pattern.is_some() {
                rt.regex = compile_regex(rt.pattern.as_ref().unwrap());
            }
        }
    }

    if let Some(manage_stopped) = &mut config.ec2_instance.manage_stopped {
        manage_stopped.dt_extract_regex = compile_regex(r"^.*\((?P<datetime>.*)\)$");
    }

    if config.rds_instance.required_tags.is_some() {
        for rt in config.rds_instance.required_tags.as_mut().unwrap() {
            if rt.pattern.is_some() {
                rt.regex = compile_regex(rt.pattern.as_ref().unwrap());
            }
        }
    }

    if config.rds_cluster.required_tags.is_some() {
        for rt in config.rds_cluster.required_tags.as_mut().unwrap() {
            if rt.pattern.is_some() {
                rt.regex = compile_regex(rt.pattern.as_ref().unwrap());
            }
        }
    }

    if config.redshift.required_tags.is_some() {
        for rt in config.redshift.required_tags.as_mut().unwrap() {
            if rt.pattern.is_some() {
                rt.regex = compile_regex(rt.pattern.as_ref().unwrap());
            }
        }
    }

    if config.emr_cluster.required_tags.is_some() {
        for rt in config.emr_cluster.required_tags.as_mut().unwrap() {
            if rt.pattern.is_some() {
                rt.regex = compile_regex(rt.pattern.as_ref().unwrap());
            }
        }
    }

    if config.glue.required_tags.is_some() {
        for rt in config.glue.required_tags.as_mut().unwrap() {
            if rt.pattern.is_some() {
                rt.regex = compile_regex(rt.pattern.as_ref().unwrap());
            }
        }
    }

    // if config.s3.required_naming_prefix.is_some() {
    //     config.s3.required_naming_regex =
    //         compile_regex(&config.s3.required_naming_prefix.as_ref().unwrap());
    // }

    config_map.insert(Client::Asg, config.asg);
    config_map.insert(Client::Ec2Instance, config.ec2_instance);
    config_map.insert(Client::Ec2Sg, config.ec2_sg);
    config_map.insert(Client::Ec2Eni, config.ec2_eni);
    config_map.insert(Client::Ec2Address, config.ec2_address);
    config_map.insert(Client::EbsVolume, config.ebs_volume);
    config_map.insert(Client::EbsSnapshot, config.ebs_snapshot);
    config_map.insert(Client::RdsInstance, config.rds_instance);
    config_map.insert(Client::RdsCluster, config.rds_cluster);
    config_map.insert(Client::EcsCluster, config.ecs);
    config_map.insert(Client::ElbAlb, config.elb_alb);
    config_map.insert(Client::ElbNlb, config.elb_nlb);
    config_map.insert(Client::EmrCluster, config.emr_cluster);
    config_map.insert(Client::EsDomain, config.es_domain);

    config_map
}

fn compile_regex(pattern: &str) -> Option<Regex> {
    match Regex::new(pattern) {
        Ok(regex) => Some(regex),
        Err(err) => {
            warn!("Failed compiling regex: {} - {:?}", pattern, err);
            None
        }
    }
}

fn default_resource_config() -> ResourceConfig {
    ResourceConfig::default()
}
