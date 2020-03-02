//! Represents a Nukable service
use crate::config::TargetState;
use colored::*;
use rusoto_core::Region;
use std::fmt;

type Result<T, E = crate::error::Error> = std::result::Result<T, E>;

#[derive(Display, Debug)]
pub enum ResourceType {
    Ec2Instance,
    Ec2Interface,
    Ec2Address,
    EbsVolume,
    EbsSnapshot,
    RDS,
    Aurora,
    S3Bucket,
    Redshift,
    EmrCluster,
    GlueDevEndpoint,
}

impl ResourceType {
    pub fn is_ec2(&self) -> bool {
        match *self {
            ResourceType::Ec2Instance | ResourceType::Ec2Interface | ResourceType::Ec2Address => {
                true
            }
            _ => false,
        }
    }

    pub fn is_instance(&self) -> bool {
        match *self {
            ResourceType::Ec2Instance => true,
            _ => false,
        }
    }

    pub fn is_volume(&self) -> bool {
        match *self {
            ResourceType::EbsVolume => true,
            _ => false,
        }
    }

    pub fn is_snapshot(&self) -> bool {
        match *self {
            ResourceType::EbsSnapshot => true,
            _ => false,
        }
    }

    pub fn is_ebs(&self) -> bool {
        match *self {
            ResourceType::EbsVolume | ResourceType::EbsSnapshot => true,
            _ => false,
        }
    }

    pub fn is_eni(&self) -> bool {
        match *self {
            ResourceType::Ec2Interface => true,
            _ => false,
        }
    }

    pub fn is_eip(&self) -> bool {
        match *self {
            ResourceType::Ec2Address => true,
            _ => false,
        }
    }

    pub fn is_rds(&self) -> bool {
        match *self {
            ResourceType::RDS => true,
            _ => false,
        }
    }

    pub fn is_aurora(&self) -> bool {
        match *self {
            ResourceType::Aurora => true,
            _ => false,
        }
    }

    pub fn is_s3(&self) -> bool {
        match *self {
            ResourceType::S3Bucket => true,
            _ => false,
        }
    }

    pub fn is_redshift(&self) -> bool {
        match *self {
            ResourceType::Redshift => true,
            _ => false,
        }
    }

    pub fn is_emr(&self) -> bool {
        match *self {
            ResourceType::EmrCluster => true,
            _ => false,
        }
    }

    pub fn is_glue(&self) -> bool {
        match *self {
            ResourceType::GlueDevEndpoint => true,
            _ => false,
        }
    }
}

#[allow(dead_code)]
pub enum FilterRule {
    /// A filter rule that checks if the required tags are provided
    /// for a given resource
    RequiredTags,
    /// A filter rule that checks to see if the resource falls under
    /// Idle (no usage)
    Idle,
    /// A filter rule that checks to see if the resource is using
    /// allowed type of the resource
    AllowedTypes,
}

#[derive(Debug, Clone)]
pub struct NTag {
    pub key: Option<String>,
    pub value: Option<String>,
}

impl fmt::Display for NTag {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.key.is_some() && self.value.is_some() {
            write!(
                f,
                "{} -> {}",
                self.key.as_ref().unwrap().on_white().black(),
                self.value.as_ref().unwrap().on_white().black()
            )
        } else {
            write!(f, "{:?} -> {:?}", self.key, self.value)
        }
    }
}

#[derive(Debug)]
pub enum EnforcementState {
    Stop,
    Delete,
    Skip,
    SkipConfig,
    SkipStopped,
}

impl EnforcementState {
    pub fn name(&self) -> colored::ColoredString {
        match *self {
            EnforcementState::Stop => "would be stopped".blue().bold(),
            EnforcementState::Delete => "would be removed".blue().bold(),
            EnforcementState::Skip => "skipped because of rules".yellow().bold(),
            EnforcementState::SkipConfig => "skipped because of config".yellow().bold(),
            EnforcementState::SkipStopped => "skipped as resource is not running".yellow().bold(),
        }
    }

    pub fn from_target_state(target_state: &TargetState) -> Self {
        if *target_state == TargetState::Deleted {
            EnforcementState::Delete
        } else {
            EnforcementState::Stop
        }
    }
}

#[derive(Debug)]
pub struct Resource {
    pub id: String,
    pub resource_type: ResourceType,
    pub region: Region,
    pub tags: Option<Vec<NTag>>,
    pub state: Option<String>,
    pub enforcement_state: EnforcementState,
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "[{}] - {} - {}",
            self.region.name().bold(),
            self.resource_type,
            self.id.bold()
        )?;

        if self.tags.is_some() && !self.tags.as_ref().unwrap().is_empty() {
            write!(f, " - {{")?;
            for tag in self.tags.as_ref().unwrap() {
                write!(f, "[{}]", tag)?;
            }
            write!(f, "}}")?;
        }

        write!(f, " - {}", self.enforcement_state.name())?;

        Ok(())
    }
}

pub trait NukeService: ::std::any::Any {
    /// Get all the resources without applying any filters
    fn scan(&self) -> Result<Vec<Resource>>;

    /// Print the resources that have been scanned
    fn print(&self, resources: &Vec<&Resource>) {
        for resource in resources {
            println!("{}", resource);
        }
    }

    /// Clean up the resources
    fn cleanup(&self, resources: &Vec<&Resource>) -> Result<()> {
        self.print(resources);

        for resource in resources {
            match resource.enforcement_state {
                EnforcementState::Stop => self.stop(resource)?,
                EnforcementState::Delete => self.delete(resource)?,
                _ => {}
            }
        }

        Ok(())
    }

    /// Stop the resource
    fn stop(&self, resource: &Resource) -> Result<()>;

    /// Delete the resource
    fn delete(&self, resource: &Resource) -> Result<()>;

    fn as_any(&self) -> &dyn ::std::any::Any;
}

pub trait RequiredTagsFilter {
    fn filter(&self) -> Result<Vec<Resource>>;
}

pub trait AllowedTypesFilter {
    fn filter(&self) -> Result<Vec<Resource>>;
}

pub trait IdleFilter {
    fn filter(&self) -> Result<Vec<Resource>>;
}
