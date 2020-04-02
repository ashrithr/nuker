//! Represents a Nuker Client
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, Resource, ResourceState};
use crate::CwClient;
use crate::Event;
use crate::NSender;
use crate::Result;
use crate::StdError;
use crate::StdResult;
use async_trait::async_trait;
use dyn_clone::DynClone;
use std::{
    fmt::{Display, Error as FmtError, Formatter},
    str::FromStr,
    sync::Arc,
};
use tracing::{error, trace};

pub const EC2_INSTANCE_TYPE: &str = "ec2_instance";
pub const EC2_SG_TYPE: &str = "ec2_sg";
pub const EC2_ENI_TYPE: &str = "ec2_eni";
pub const EC2_ADDRESS_TYPE: &str = "ec2_address";
pub const EBS_VOL_TYPE: &str = "ebs_volume";
pub const RDS_INSTANCE_TYPE: &str = "rds";
pub const RDS_CLUSTER_TYPE: &str = "rds_aurora";
pub const S3_TYPE: &str = "s3";
pub const EMR_TYPE: &str = "emr";
pub const GLUE_TYPE: &str = "glue";
pub const SAGEMAKER_TYPE: &str = "sagemaker";
pub const REDSHIFT_TYPE: &str = "redshift";
pub const ES_TYPE: &str = "es";
pub const ELB_ALB_TYPE: &str = "elb_alb";
pub const ELB_NLB_TYPE: &str = "elb_nlb";
pub const ASG_TYPE: &str = "asg";
pub const ECS_TYPE: &str = "ecs";
pub const VPC_TYPE: &str = "vpc";
pub const DEFAULT_TYPE: &str = "default";

pub type ClientType = Client;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Client {
    EbsVolume,
    Ec2Instance,
    Ec2Sg,
    Ec2Address,
    Ec2Eni,
    ElbAlb,
    ElbNlb,
    EmrCluster,
    EsDomain,
    Glue,
    RdsInstance,
    RdsCluster,
    Redshift,
    S3,
    Sagemaker,
    Asg,
    EcsCluster,
    Vpc,
    DefaultClient,
}

impl Client {
    pub fn is_default(&self) -> bool {
        match *&self {
            Client::DefaultClient => true,
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ParseClientError {
    message: String,
}

impl ParseClientError {
    /// Parses a region given as a string literal into a type `Region'
    pub fn new(input: &str) -> Self {
        ParseClientError {
            message: format!("Not a valid supported Client: {}", input),
        }
    }
}

impl StdError for ParseClientError {}

impl Display for ParseClientError {
    fn fmt(&self, f: &mut Formatter) -> StdResult<(), FmtError> {
        write!(f, "{}", self.message)
    }
}

impl FromStr for Client {
    type Err = ParseClientError;

    fn from_str(s: &str) -> StdResult<Client, ParseClientError> {
        let v: &str = &s.to_lowercase();
        match v {
            RDS_CLUSTER_TYPE => Ok(Client::RdsCluster),
            EBS_VOL_TYPE => Ok(Client::EbsVolume),
            EC2_INSTANCE_TYPE => Ok(Client::Ec2Instance),
            EC2_SG_TYPE => Ok(Client::Ec2Sg),
            EC2_ENI_TYPE => Ok(Client::Ec2Eni),
            EC2_ADDRESS_TYPE => Ok(Client::Ec2Address),
            ELB_ALB_TYPE => Ok(Client::ElbAlb),
            ELB_NLB_TYPE => Ok(Client::ElbNlb),
            EMR_TYPE => Ok(Client::EmrCluster),
            ES_TYPE => Ok(Client::EsDomain),
            GLUE_TYPE => Ok(Client::Glue),
            RDS_INSTANCE_TYPE => Ok(Client::RdsInstance),
            REDSHIFT_TYPE => Ok(Client::Redshift),
            S3_TYPE => Ok(Client::S3),
            SAGEMAKER_TYPE => Ok(Client::Sagemaker),
            ASG_TYPE => Ok(Client::Asg),
            ECS_TYPE => Ok(Client::EcsCluster),
            VPC_TYPE => Ok(Client::Vpc),
            s => Err(ParseClientError::new(s)),
        }
    }
}

impl Client {
    pub fn name(&self) -> &str {
        match *self {
            Client::RdsCluster => RDS_CLUSTER_TYPE,
            Client::EbsVolume => EBS_VOL_TYPE,
            Client::Ec2Instance => EC2_INSTANCE_TYPE,
            Client::Ec2Sg => EC2_SG_TYPE,
            Client::Ec2Eni => EC2_ENI_TYPE,
            Client::Ec2Address => EC2_ADDRESS_TYPE,
            Client::ElbAlb => ELB_ALB_TYPE,
            Client::ElbNlb => ELB_NLB_TYPE,
            Client::EmrCluster => EMR_TYPE,
            Client::EsDomain => ES_TYPE,
            Client::Glue => GLUE_TYPE,
            Client::RdsInstance => RDS_INSTANCE_TYPE,
            Client::Redshift => REDSHIFT_TYPE,
            Client::S3 => S3_TYPE,
            Client::Sagemaker => SAGEMAKER_TYPE,
            Client::Asg => ASG_TYPE,
            Client::EcsCluster => ECS_TYPE,
            Client::Vpc => VPC_TYPE,
            Client::DefaultClient => DEFAULT_TYPE,
        }
    }

    pub fn iter() -> impl Iterator<Item = Client> {
        [
            Client::RdsCluster,
            Client::EbsVolume,
            Client::Ec2Instance,
            Client::Ec2Sg,
            Client::Ec2Eni,
            Client::Ec2Address,
            Client::ElbAlb,
            Client::ElbNlb,
            Client::EmrCluster,
            Client::EsDomain,
            Client::Glue,
            Client::RdsInstance,
            Client::Redshift,
            Client::S3,
            Client::Sagemaker,
            Client::Asg,
            Client::EcsCluster,
            Client::Vpc,
        ]
        .iter()
        .copied()
    }
}

#[async_trait]
pub trait NukerClient: Send + Sync + DynClone {
    /// Scans for all the resources that are scannable by a Resource Scanner
    /// before applying any Filter's and Rule's
    async fn scan(&self) -> Result<Vec<Resource>>;

    /// Find dependent resources for a given Resource
    async fn dependencies(&self, resource: &Resource) -> Option<Vec<Resource>>;

    /// Publishes the resources to shared Channel
    async fn publish(
        &self,
        mut tx: NSender<Event>,
        c: Client,
        config: ResourceConfig,
        cw_client: Arc<Box<CwClient>>,
    ) {
        if let Ok(resources) = self.scan().await {
            for mut resource in resources {
                resource.dependencies = self.dependencies(&resource).await;
                resource.enforcement_state = self
                    .filter_resource(&resource, &config, cw_client.clone())
                    .await;

                if let Err(err) = tx.send(Event::Resource(resource)).await {
                    error!(err = ?err, "Failed to publish event to the queue");
                }
            }
        }

        match tx.send(Event::Shutdown(c)).await {
            Ok(()) => trace!("Complete Resource Scanner"),
            Err(err) => error!(err = ?err, "Failed sending shutdown event"),
        }
    }

    /// Checks to see if the required tags for a particular resource exists or
    /// not.
    fn filter_by_tags(&self, resource: &Resource, config: &ResourceConfig) -> bool {
        if let Some(ref rt) = config.required_tags {
            let tags = resource.tags.clone();
            crate::util::compare_tags(tags, rt)
        } else {
            false
        }
    }

    /// Filters a resource based on its start time
    fn filter_by_runtime(&self, resource: &Resource, config: &ResourceConfig) -> bool {
        if let (Some(ref older_than), Some(st)) =
            (config.max_run_time, resource.start_time.as_deref())
        {
            crate::util::is_ts_older_than(st, older_than)
        } else {
            false
        }
    }

    /// Filters a resource based on its type and types that are allowed
    fn filter_by_allowed_types(&self, resource: &Resource, config: &ResourceConfig) -> bool {
        if let (Some(allowed), Some(type_)) = (
            config.allowed_types.as_deref(),
            resource.resource_type.as_ref(),
        ) {
            type_.iter().all(|t_| allowed.contains(&t_))
        } else {
            false
        }
    }

    /// Filters a resource based the provided whitelist
    fn filter_by_whitelist(&self, resource: &Resource, config: &ResourceConfig) -> bool {
        if let Some(ref whitelist) = config.whitelist {
            if whitelist.contains(&resource.id) {
                return true;
            }
        }
        false
    }

    /// Filters a resource that are not running
    fn filter_by_state(&self, resource: &Resource) -> bool {
        if let Some(ref state) = resource.state {
            match state {
                ResourceState::Running => false,
                _ => true,
            }
        } else {
            false
        }
    }

    /// Filters a resource based on its idle rules (Cloudwatch metrics)
    async fn filter_by_idle_rules(
        &self,
        resource: &Resource,
        config: &ResourceConfig,
        cw_client: Arc<Box<CwClient>>,
    ) -> bool {
        if let Some(ref _rules) = config.idle_rules {
            match resource.type_ {
                Client::Ec2Instance => cw_client.filter_instance(resource.id.as_str()).await,
                Client::EbsVolume => cw_client.filter_volume(resource.id.as_str()).await,
                Client::RdsInstance => cw_client.filter_db_instance(resource.id.as_str()).await,
                Client::RdsCluster => cw_client.filter_db_cluster(resource.id.as_str()).await,
                Client::Redshift => cw_client.filter_rs_cluster(resource.id.as_str()).await,
                Client::EsDomain => cw_client.filter_es_domain(resource.id.as_str()).await,
                Client::ElbAlb => {
                    cw_client
                        .filter_alb_load_balancer(resource.id.as_str())
                        .await
                }
                Client::ElbNlb => {
                    cw_client
                        .filter_nlb_load_balancer(resource.id.as_str())
                        .await
                }
                Client::EcsCluster => cw_client.filter_ecs_cluster(resource.id.as_str()).await,
                Client::EmrCluster => cw_client.filter_emr_cluster(resource.id.as_str()).await,
                _ => false,
            }
        } else {
            false
        }
    }

    /// Additional filters to apply that are not generic for all resource types
    fn additional_filters(&self, resource: &Resource, config: &ResourceConfig) -> bool;

    /// Filters a provided resource by applying all the filters
    async fn filter_resource(
        &self,
        resource: &Resource,
        config: &ResourceConfig,
        cw_client: Arc<Box<CwClient>>,
    ) -> EnforcementState {
        if self.filter_by_whitelist(resource, config) {
            // Skip a resource if its in the whitelist
            EnforcementState::SkipConfig
        } else if self.filter_by_tags(resource, config) {
            // Enforce provided required tags
            trace!(
                resource = resource.id.as_str(),
                "Resource tags does not match"
            );
            EnforcementState::from_target_state(&config.target_state)
        } else if self.filter_by_allowed_types(resource, config) {
            // Enforce allowed types
            trace!(
                resource = resource.id.as_str(),
                "Resource is not in list of allowed types"
            );
            EnforcementState::from_target_state(&config.target_state)
        } else if self.filter_by_state(resource) {
            // Skip resource if its state is stopped
            EnforcementState::SkipStopped
        } else if self.filter_by_runtime(resource, config) {
            // Enforce max runtime for a resource if max_run_time is provided
            trace!(
                resource = resource.id.as_str(),
                "Resource exceeded max runtime"
            );
            EnforcementState::from_target_state(&config.target_state)
        } else if self.filter_by_idle_rules(resource, config, cw_client).await {
            // Enforce Idle rules
            trace!(resource = resource.id.as_str(), "Resource is idle");
            EnforcementState::from_target_state(&config.target_state)
        } else if self.additional_filters(resource, config) {
            // Apply any additional filters that are implemented by
            // Resource clients.
            trace!(
                resource = resource.id.as_str(),
                "Resource does not meet additional filters"
            );
            EnforcementState::from_target_state(&config.target_state)
        } else {
            EnforcementState::Skip
        }
    }

    async fn cleanup(&self, resource: &Resource) -> Result<()> {
        match resource.enforcement_state {
            EnforcementState::Stop => self.stop(resource).await?,
            EnforcementState::Delete => self.delete(resource).await?,
            _ => {}
        }

        Ok(())
    }

    /// Stop the resource
    async fn stop(&self, resource: &Resource) -> Result<()>;

    /// Delete the resource
    async fn delete(&self, resource: &Resource) -> Result<()>;
}

dyn_clone::clone_trait_object!(NukerClient);
