//! Represents a Nukable service
use colored::*;
use rusoto_core::Region;
use std::fmt;

type Result<T, E = crate::error::Error> = std::result::Result<T, E>;

#[derive(Display, Debug)]
pub enum ResourceType {
    Ec2Instance,
    Ec2Volume,
    RDS,
    Aurora,
    S3Bucket,
    Redshift,
}

impl ResourceType {
    pub fn is_ec2(&self) -> bool {
        match *self {
            ResourceType::Ec2Instance | ResourceType::Ec2Volume => true,
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
            ResourceType::Ec2Volume => true,
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

#[derive(Debug)]
pub struct NTag {
    pub key: Option<String>,
    pub value: Option<String>,
}

#[derive(Debug)]
pub enum EnforcementState {
    Stop,
    Delete,
    Skip,
    SkipConfig,
}

impl EnforcementState {
    pub fn name(&self) -> colored::ColoredString {
        match *self {
            EnforcementState::Stop => "would be stopped".blue().bold(),
            EnforcementState::Delete => "would be removed".blue().bold(),
            EnforcementState::Skip => "skipped because of rules".yellow().bold(),
            EnforcementState::SkipConfig => "skipped because of config".yellow().bold(),
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
            "[{}] - {} - {} - {}",
            self.region.name().bold(),
            self.resource_type,
            self.id.bold(),
            self.enforcement_state.name()
        )
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

    fn as_any(&self) -> &dyn::std::any::Any;
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
