//! Configuration Parser
use clap::{App, Arg};
use serde_derive::Deserialize;
use std::fs::File;
use std::io::Read;
use std::time::Duration;

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

/// Cli Args
#[derive(Debug, Clone)]
pub struct Args {
    config: String,
    profile: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    dry_run: bool,
    verbose: u64,
}

/// Configuration struct for aws-nuke executable
///
/// This struct is built from reading the configuration file
#[derive(Debug, Deserialize)]
pub struct Config {
    pub profile: Option<String>,
    pub regions: Vec<String>,
    pub dry_run: bool,
    pub print_usage: bool,
    pub usage_days: i64,
    pub ec2: Ec2Config,
    pub rds: RdsConfig,
    pub aurora: AuroraConfig,
    pub s3: S3Config,
    pub redshift: RedshiftConfig,
}

#[derive(Debug, Deserialize, Clone, Eq, PartialEq)]
pub enum TargetState {
    Stopped,
    Deleted,
}

#[derive(Debug, Deserialize, Clone)]
pub struct IdleRules {
    enabled: bool,
    pub min_utilization: f32,
    #[serde(with = "humantime_serde")]
    pub min_duration: Duration,
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

#[derive(Debug, Deserialize, Clone)]
pub struct EbsCleanup {
    pub enabled: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SecurityGroups {
    pub enabled: bool,
    pub source_cidr: Vec<String>,
    pub from_port: u16,
    pub to_port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Ec2Config {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Vec<String>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    pub idle_rules: IdleRules,
    pub termination_protection: TerminationProtection,
    pub ebs_cleanup: EbsCleanup,
    pub security_groups: SecurityGroups,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RdsConfig {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Vec<String>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    pub idle_rules: IdleRules,
    pub termination_protection: TerminationProtection,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuroraConfig {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Vec<String>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    pub idle_rules: IdleRules,
    pub termination_protection: TerminationProtection,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RedshiftConfig {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Vec<String>,
    pub allowed_instance_types: Vec<String>,
    pub ignore: Vec<String>,
    pub idle_rules: IdleRules,
}

#[derive(Debug, Deserialize, Clone)]
pub struct S3Config {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_naming_prefix: String,
    pub ignore: Vec<String>,
}

/// Parse the command line arguments for aws-nuke executable
pub fn parse_args() -> (u64, String) {
    let args = App::new("aws-nuke")
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

    (verbose, args.value_of("config-file").unwrap().to_string())
}

/// Parses the aws-nuke configuration file
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
    toml::from_str(buffer).expect("could not parse toml configuration")
}
