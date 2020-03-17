//! Configuration Parser
use crate::service::Service;
use clap::{App, Arg};
use regex::Regex;
use serde::Deserialize;
use std::{fs::File, io::Read, str::FromStr, time::Duration};
use tracing::warn;

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

/// Cli Args
#[derive(Debug, Clone)]
pub struct Args {
    pub config: String,
    pub profile: Option<String>,
    pub regions: Vec<String>,
    pub targets: Option<Vec<Service>>,
    pub exclude: Option<Vec<Service>>,
    pub dry_run: bool,
    pub force: bool,
    pub verbose: u64,
    pub version: String,
}

/// Configuration struct for nuker executable
///
/// This struct is built from reading the configuration file
#[derive(Debug, Deserialize)]
pub struct Config {
    pub ec2: Ec2Config,
    pub ebs: EbsConfig,
    pub elb: ElbConfig,
    pub rds: RdsConfig,
    pub aurora: AuroraConfig,
    pub s3: S3Config,
    pub emr: EmrConfig,
    pub redshift: RedshiftConfig,
    pub glue: GlueConfig,
    pub sagemaker: SagemakerConfig,
    pub es: EsConfig,
    pub asg: AutoScalingConfig,
    pub ecs: EcsConfig,
}

#[derive(Debug, Deserialize, Clone, Eq, PartialEq)]
pub enum TargetState {
    Stopped,
    Deleted,
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
struct ManageStoppedInstances {
    enabled: bool,
    #[serde(with = "humantime_serde")]
    terminate_older_than: Duration,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TerminationProtection {
    pub ignore: bool,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct SecurityGroups {
    pub enabled: bool,
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
pub struct Ec2Config {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    pub idle_rules: Option<Vec<IdleRules>>,
    pub termination_protection: TerminationProtection,
    pub security_groups: SecurityGroups,
    pub eni: Eni,
    pub eip: Eip,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EbsConfig {
    pub enabled: bool,
    pub target_state: TargetState,
    pub ignore: Vec<String>,
    pub idle_rules: Option<Vec<IdleRules>>,
    #[serde(with = "humantime_serde")]
    pub older_than: Option<Duration>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EmrConfig {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    pub idle_rules: Option<Vec<IdleRules>>,
    pub termination_protection: TerminationProtection,
    pub security_groups: SecurityGroups,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RdsConfig {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    pub idle_rules: Option<Vec<IdleRules>>,
    pub termination_protection: TerminationProtection,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuroraConfig {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    pub idle_rules: Option<Vec<IdleRules>>,
    pub termination_protection: TerminationProtection,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RedshiftConfig {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    pub idle_rules: Option<Vec<IdleRules>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct S3Config {
    pub enabled: bool,
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
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub ignore: Vec<String>,
    #[serde(with = "humantime_serde")]
    pub older_than: Duration,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SagemakerConfig {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    #[serde(with = "humantime_serde")]
    pub older_than: Duration,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EsConfig {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    pub idle_rules: Option<Vec<IdleRules>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ElbConfig {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub ignore: Vec<String>,
    pub alb_idle_rules: Option<Vec<IdleRules>>,
    pub nlb_idle_rules: Option<Vec<IdleRules>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AutoScalingConfig {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub ignore: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EcsConfig {
    pub enabled: bool,
    pub target_state: TargetState,
    pub allowed_instance_types: Vec<String>,
    pub required_tags: Option<Vec<RequiredTags>>,
    pub ignore: Vec<String>,
    pub idle_rules: Option<Vec<IdleRules>>,
}

/// Parse the command line arguments for nuker executable
pub fn parse_args() -> Args {
    let args = App::new("nuker")
        .version(VERSION.unwrap_or("unknown"))
        .arg(
            Arg::with_name("config-file")
                .long("config")
                .short("C")
                .value_name("config")
                .required(true)
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

    let targets: Option<Vec<Service>> = if args.is_present("target") {
        Some(
            args.values_of("target")
                .unwrap()
                .map(|t| Service::from_str(t).unwrap())
                .collect(),
        )
    } else {
        None
    };

    let exclude: Option<Vec<Service>> = if args.is_present("exclude") {
        Some(
            args.values_of("exclude")
                .unwrap()
                .map(|e| Service::from_str(e).unwrap())
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
    let mut config: Config = toml::from_str(buffer).expect("could not parse toml configuration");

    // Compile all regex expressions up front
    if config.ec2.required_tags.is_some() {
        for rt in config.ec2.required_tags.as_mut().unwrap() {
            if rt.pattern.is_some() {
                rt.regex = compile_regex(rt.pattern.as_ref().unwrap());
            }
        }
    }

    if config.rds.required_tags.is_some() {
        for rt in config.rds.required_tags.as_mut().unwrap() {
            if rt.pattern.is_some() {
                rt.regex = compile_regex(rt.pattern.as_ref().unwrap());
            }
        }
    }

    if config.aurora.required_tags.is_some() {
        for rt in config.aurora.required_tags.as_mut().unwrap() {
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

    if config.emr.required_tags.is_some() {
        for rt in config.emr.required_tags.as_mut().unwrap() {
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

    if config.s3.enabled && config.s3.required_naming_prefix.is_some() {
        config.s3.required_naming_regex =
            compile_regex(&config.s3.required_naming_prefix.as_ref().unwrap());
    }

    config
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
