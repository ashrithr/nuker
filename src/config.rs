//! Configuration Parser
use clap::{App, Arg};
use serde_derive::Deserialize;
use std::fs::File;
use std::io::Read;
use std::time::Duration;

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

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
}

#[derive(Debug, Deserialize, Clone)]
pub enum TargetState {
    Stopped,
    Terminated,
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
pub struct Ec2Config {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Vec<String>,
    pub allowed_instance_types: Vec<String>,
    pub idle_rules: IdleRules,
    pub termination_protection: TerminationProtection,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RdsConfig {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Vec<String>,
    pub allowed_instance_types: Vec<String>,
    pub idle_rules: IdleRules,
    pub termination_protection: TerminationProtection,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuroraConfig {
    pub enabled: bool,
    pub target_state: TargetState,
    pub required_tags: Vec<String>,
    pub allowed_instance_types: Vec<String>,
    pub idle_rules: IdleRules,
    pub termination_protection: TerminationProtection,
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
