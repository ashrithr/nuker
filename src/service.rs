//! Represents a Nukable service
use crate::resource::{EnforcementState, Resource};
use async_trait::async_trait;
use dyn_clone::DynClone;
use std::{
    any::Any,
    error::Error,
    fmt::{Display, Error as FmtError, Formatter},
    str::FromStr,
};

type Result<T, E = crate::error::Error> = std::result::Result<T, E>;

pub const EC2_TYPE: &str = "ec2";
pub const EBS_TYPE: &str = "ebs";
pub const RDS_TYPE: &str = "rds";
pub const AURORA_TYPE: &str = "aurora";
pub const S3_TYPE: &str = "s3";
pub const EMR_TYPE: &str = "emr";
pub const GLUE_TYPE: &str = "glue";
pub const SAGEMAKER_TYPE: &str = "sagemaker";
pub const REDSHIFT_TYPE: &str = "redshift";
pub const ES_TYPE: &str = "es";
pub const ELB_TYPE: &str = "elb";
pub const ASG_TYPE: &str = "asg";
pub const ECS_TYPE: &str = "ecs";
pub const VPC_TYPE: &str = "vpc";

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Service {
    Aurora,
    Ebs,
    Ec2,
    Elb,
    Emr,
    Es,
    Glue,
    Rds,
    Redshift,
    S3,
    Sagemaker,
    Asg,
    Ecs,
    Vpc,
}

#[derive(Debug, PartialEq)]
pub struct ParseServiceError {
    message: String,
}

impl ParseServiceError {
    /// Parses a region given as a string literal into a type `Region'
    pub fn new(input: &str) -> Self {
        ParseServiceError {
            message: format!("Not a valid supported service: {}", input),
        }
    }
}

impl Error for ParseServiceError {}

impl Display for ParseServiceError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        write!(f, "{}", self.message)
    }
}

impl FromStr for Service {
    type Err = ParseServiceError;

    fn from_str(s: &str) -> Result<Service, ParseServiceError> {
        let v: &str = &s.to_lowercase();
        match v {
            AURORA_TYPE => Ok(Service::Aurora),
            EBS_TYPE => Ok(Service::Ebs),
            EC2_TYPE => Ok(Service::Ec2),
            ELB_TYPE => Ok(Service::Elb),
            EMR_TYPE => Ok(Service::Emr),
            ES_TYPE => Ok(Service::Es),
            GLUE_TYPE => Ok(Service::Glue),
            RDS_TYPE => Ok(Service::Rds),
            REDSHIFT_TYPE => Ok(Service::Redshift),
            S3_TYPE => Ok(Service::S3),
            SAGEMAKER_TYPE => Ok(Service::Sagemaker),
            ASG_TYPE => Ok(Service::Asg),
            ECS_TYPE => Ok(Service::Ecs),
            VPC_TYPE => Ok(Service::Vpc),
            s => Err(ParseServiceError::new(s)),
        }
    }
}

impl Service {
    pub fn name(&self) -> &str {
        match *self {
            Service::Aurora => AURORA_TYPE,
            Service::Ebs => EBS_TYPE,
            Service::Ec2 => EC2_TYPE,
            Service::Elb => ELB_TYPE,
            Service::Emr => EMR_TYPE,
            Service::Es => ES_TYPE,
            Service::Glue => GLUE_TYPE,
            Service::Rds => RDS_TYPE,
            Service::Redshift => REDSHIFT_TYPE,
            Service::S3 => S3_TYPE,
            Service::Sagemaker => SAGEMAKER_TYPE,
            Service::Asg => ASG_TYPE,
            Service::Ecs => ECS_TYPE,
            Service::Vpc => VPC_TYPE,
        }
    }

    pub fn iter() -> impl Iterator<Item = Service> {
        [
            Service::Aurora,
            Service::Ebs,
            Service::Ec2,
            Service::Elb,
            Service::Emr,
            Service::Es,
            Service::Glue,
            Service::Rds,
            Service::Redshift,
            Service::S3,
            Service::Sagemaker,
            Service::Asg,
            Service::Ecs,
            Service::Vpc,
        ]
        .iter()
        .copied()
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

#[async_trait]
pub trait NukerService: Any + Send + Sync + DynClone {
    /// Get all the resources without applying any filters
    async fn scan(&self) -> Result<Vec<Resource>>;

    /// Clean up the resources, based on the enforcement state figured out
    /// from the specified rules
    async fn cleanup(&self, resources: Vec<Resource>) -> Result<()> {
        for resource in resources {
            match resource.enforcement_state {
                EnforcementState::Stop => self.stop(&resource).await?,
                EnforcementState::Delete => self.delete(&resource).await?,
                _ => {}
            }
        }

        Ok(())
    }

    /// Stop the resource
    async fn stop(&self, resource: &Resource) -> Result<()>;

    /// Delete the resource
    async fn delete(&self, resource: &Resource) -> Result<()>;

    fn as_any(&self) -> &dyn ::std::any::Any;
}

dyn_clone::clone_trait_object!(NukerService);

pub trait RequiredTagsFilter {
    fn filter(&self) -> Result<Vec<Resource>>;
}

pub trait AllowedTypesFilter {
    fn filter(&self) -> Result<Vec<Resource>>;
}

pub trait IdleFilter {
    fn filter(&self) -> Result<Vec<Resource>>;
}
