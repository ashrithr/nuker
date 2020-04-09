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
use tracing::{debug, error, trace};

pub const ASG_TYPE: &str = "asg";
pub const DEFAULT_TYPE: &str = "default";
pub const EBS_SNAP_TYPE: &str = "ebs_snapshot";
pub const EBS_VOL_TYPE: &str = "ebs_volume";
pub const EC2_ADDRESS_TYPE: &str = "ec2_address";
pub const EC2_ENI_TYPE: &str = "ec2_eni";
pub const EC2_INSTANCE_TYPE: &str = "ec2_instance";
pub const EC2_SG_TYPE: &str = "ec2_sg";
pub const EC2_VPC_TYPE: &str = "ec2_vpc";
pub const EC2_IGW_TYPE: &str = "ec2_igw";
pub const EC2_SUBNET_TYPE: &str = "ec2_subnet";
pub const EC2_RT_TYPE: &str = "ec2_rt";
pub const EC2_NACL_TYPE: &str = "ec2_network_acl";
pub const EC2_NATGW_TYPE: &str = "ec2_nat_gw";
pub const EC2_VPNGW_TYPE: &str = "ec2_vpn_gw";
pub const EC2_VPC_ENDPOINT_TYPE: &str = "ec2_vpc_endpoint";
pub const EC2_PEERING_CONNECTION: &str = "ec2_peering_connection";
pub const ECS_TYPE: &str = "ecs";
pub const ELB_ALB_TYPE: &str = "elb_alb";
pub const ELB_NLB_TYPE: &str = "elb_nlb";
pub const EMR_TYPE: &str = "emr_cluster";
pub const ES_TYPE: &str = "es_domain";
pub const GLUE_ENDPOINT_TYPE: &str = "glue_endpoint";
pub const RDS_CLUSTER_TYPE: &str = "rds_aurora";
pub const RDS_INSTANCE_TYPE: &str = "rds";
pub const REDSHIFT_CLUSTER_TYPE: &str = "rs_cluster";
pub const S3_BUCKET_TYPE: &str = "s3_bucket";
pub const SAGEMAKER_NOTEBOOK_TYPE: &str = "sagemaker_notebook";

pub type ClientType = Client;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Client {
    Asg,
    DefaultClient,
    EbsSnapshot,
    EbsVolume,
    Ec2Address,
    Ec2Eni,
    Ec2Instance,
    Ec2Sg,
    Ec2Vpc,
    Ec2Igw,
    Ec2Subnet,
    Ec2RouteTable,
    Ec2NetworkACL,
    Ec2NatGW,
    Ec2VpnGW,
    Ec2VpcEndpoint,
    Ec2PeeringConnection,
    EcsCluster,
    ElbAlb,
    ElbNlb,
    EmrCluster,
    EsDomain,
    GlueEndpoint,
    RdsCluster,
    RdsInstance,
    RsCluster,
    S3Bucket,
    SagemakerNotebook,
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
            ASG_TYPE => Ok(Client::Asg),
            EBS_SNAP_TYPE => Ok(Client::EbsSnapshot),
            EBS_VOL_TYPE => Ok(Client::EbsVolume),
            EC2_ADDRESS_TYPE => Ok(Client::Ec2Address),
            EC2_ENI_TYPE => Ok(Client::Ec2Eni),
            EC2_INSTANCE_TYPE => Ok(Client::Ec2Instance),
            EC2_SG_TYPE => Ok(Client::Ec2Sg),
            EC2_VPC_TYPE => Ok(Client::Ec2Vpc),
            EC2_IGW_TYPE => Ok(Client::Ec2Igw),
            EC2_SUBNET_TYPE => Ok(Client::Ec2Subnet),
            EC2_RT_TYPE => Ok(Client::Ec2RouteTable),
            EC2_NACL_TYPE => Ok(Client::Ec2NetworkACL),
            EC2_NATGW_TYPE => Ok(Client::Ec2NatGW),
            EC2_VPNGW_TYPE => Ok(Client::Ec2VpnGW),
            EC2_VPC_ENDPOINT_TYPE => Ok(Client::Ec2VpcEndpoint),
            EC2_PEERING_CONNECTION => Ok(Client::Ec2PeeringConnection),
            ECS_TYPE => Ok(Client::EcsCluster),
            ELB_ALB_TYPE => Ok(Client::ElbAlb),
            ELB_NLB_TYPE => Ok(Client::ElbNlb),
            EMR_TYPE => Ok(Client::EmrCluster),
            ES_TYPE => Ok(Client::EsDomain),
            GLUE_ENDPOINT_TYPE => Ok(Client::GlueEndpoint),
            RDS_CLUSTER_TYPE => Ok(Client::RdsCluster),
            RDS_INSTANCE_TYPE => Ok(Client::RdsInstance),
            REDSHIFT_CLUSTER_TYPE => Ok(Client::RsCluster),
            S3_BUCKET_TYPE => Ok(Client::S3Bucket),
            SAGEMAKER_NOTEBOOK_TYPE => Ok(Client::SagemakerNotebook),
            s => Err(ParseClientError::new(s)),
        }
    }
}

impl Client {
    pub fn name(&self) -> &str {
        match *self {
            Client::Asg => ASG_TYPE,
            Client::DefaultClient => DEFAULT_TYPE,
            Client::EbsSnapshot => EBS_SNAP_TYPE,
            Client::EbsVolume => EBS_VOL_TYPE,
            Client::Ec2Address => EC2_ADDRESS_TYPE,
            Client::Ec2Eni => EC2_ENI_TYPE,
            Client::Ec2Instance => EC2_INSTANCE_TYPE,
            Client::Ec2Sg => EC2_SG_TYPE,
            Client::Ec2Vpc => EC2_VPC_TYPE,
            Client::Ec2Igw => EC2_IGW_TYPE,
            Client::Ec2Subnet => EC2_SUBNET_TYPE,
            Client::Ec2RouteTable => EC2_RT_TYPE,
            Client::Ec2NetworkACL => EC2_NACL_TYPE,
            Client::Ec2NatGW => EC2_NATGW_TYPE,
            Client::Ec2VpnGW => EC2_VPNGW_TYPE,
            Client::Ec2VpcEndpoint => EC2_VPC_ENDPOINT_TYPE,
            Client::Ec2PeeringConnection => EC2_PEERING_CONNECTION,
            Client::EcsCluster => ECS_TYPE,
            Client::ElbAlb => ELB_ALB_TYPE,
            Client::ElbNlb => ELB_NLB_TYPE,
            Client::EmrCluster => EMR_TYPE,
            Client::EsDomain => ES_TYPE,
            Client::GlueEndpoint => GLUE_ENDPOINT_TYPE,
            Client::RdsCluster => RDS_CLUSTER_TYPE,
            Client::RdsInstance => RDS_INSTANCE_TYPE,
            Client::RsCluster => REDSHIFT_CLUSTER_TYPE,
            Client::S3Bucket => S3_BUCKET_TYPE,
            Client::SagemakerNotebook => SAGEMAKER_NOTEBOOK_TYPE,
        }
    }

    pub fn iter() -> impl Iterator<Item = Client> {
        [
            Client::Asg,
            Client::EbsSnapshot,
            Client::EbsVolume,
            Client::Ec2Address,
            Client::Ec2Eni,
            Client::Ec2Igw,
            Client::Ec2Instance,
            Client::Ec2NatGW,
            Client::Ec2NetworkACL,
            Client::Ec2PeeringConnection,
            Client::Ec2RouteTable,
            Client::Ec2Sg,
            Client::Ec2Subnet,
            Client::Ec2Vpc,
            Client::Ec2VpcEndpoint,
            Client::Ec2VpnGW,
            Client::EcsCluster,
            Client::ElbAlb,
            Client::ElbNlb,
            Client::EmrCluster,
            Client::EsDomain,
            Client::GlueEndpoint,
            Client::RdsCluster,
            Client::RdsInstance,
            Client::RsCluster,
            Client::S3Bucket,
            Client::SagemakerNotebook,
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
                let enforcement_state = self
                    .filter_resource(&resource, &config, cw_client.clone())
                    .await;
                if enforcement_state == EnforcementState::Delete
                    || enforcement_state == EnforcementState::DeleteDependent
                {
                    resource.dependencies = self.dependencies(&resource).await;
                }
                resource.enforcement_state = enforcement_state;

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
            !type_.iter().all(|t_| allowed.contains(&t_))
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

    /// Filters a resource that is not running
    fn filter_by_state(&self, resource: &Resource) -> bool {
        if let Some(ref state) = resource.state {
            match state {
                // only enforce rules on resources that are in running or available state
                ResourceState::Running | ResourceState::Available => false,
                // ignore all other resource states
                _ => true,
            }
        } else {
            false
        }
    }

    /// Filters a resource based on its naming prefix (specifically for S3 buckets)
    fn filter_by_naming_prefix(&self, resource: &Resource, config: &ResourceConfig) -> bool {
        if let Some(ref np) = config.naming_prefix {
            !np.regex.as_ref().unwrap().is_match(&resource.id)
        } else {
            false
        }
    }

    /// Filters a resource based on its idle rules (Cloudwatch metrics)
    async fn filter_by_idle_rules(
        &self,
        resource: &Resource,
        cw_client: Arc<Box<CwClient>>,
    ) -> bool {
        match resource.type_ {
            Client::Ec2Instance => cw_client.filter_instance(resource.id.as_str()).await,
            Client::EbsVolume => cw_client.filter_volume(resource.id.as_str()).await,
            Client::RdsInstance => cw_client.filter_db_instance(resource.id.as_str()).await,
            Client::RdsCluster => cw_client.filter_db_cluster(resource.id.as_str()).await,
            Client::RsCluster => cw_client.filter_rs_cluster(resource.id.as_str()).await,
            Client::EsDomain => cw_client.filter_es_domain(resource.id.as_str()).await,
            Client::ElbAlb => {
                let dimension_val = format!(
                    "app/{}/{}",
                    resource.id.as_str(),
                    resource.arn.as_deref().unwrap().split('/').last().unwrap()
                );
                cw_client
                    .filter_alb_load_balancer(dimension_val.as_str())
                    .await
            }
            Client::ElbNlb => {
                let dimension_val = format!(
                    "app/{}/{}",
                    resource.id.as_str(),
                    resource.arn.as_deref().unwrap().split('/').last().unwrap()
                );
                cw_client
                    .filter_nlb_load_balancer(dimension_val.as_str())
                    .await
            }
            Client::EcsCluster => cw_client.filter_ecs_cluster(resource.id.as_str()).await,
            Client::EmrCluster => cw_client.filter_emr_cluster(resource.id.as_str()).await,
            _ => false,
        }
    }

    /// Additional filters to apply that are not generic for all resource types
    async fn additional_filters(
        &self,
        resource: &Resource,
        config: &ResourceConfig,
    ) -> Option<bool>;

    /// Filters a provided resource by applying all the filters
    async fn filter_resource(
        &self,
        resource: &Resource,
        config: &ResourceConfig,
        cw_client: Arc<Box<CwClient>>,
    ) -> EnforcementState {
        if resource.enforcement_state == EnforcementState::SkipUnknownState {
            if self.filter_by_whitelist(resource, config) {
                // Skip a resource if its in the whitelist
                debug!(resource = resource.id.as_str(), "Resource whitelisted");
                EnforcementState::SkipConfig
            } else if self.filter_by_state(resource) {
                // Skip resource if its state is stopped
                EnforcementState::SkipStopped
            } else if self.filter_by_tags(resource, config) {
                // Enforce provided required tags
                debug!(
                    resource = resource.id.as_str(),
                    "Resource tags does not match."
                );
                EnforcementState::from_target_state(&config.target_state)
            } else if self.filter_by_allowed_types(resource, config) {
                // Enforce allowed types
                debug!(
                    resource = resource.id.as_str(),
                    "Resource is not in list of allowed types."
                );
                EnforcementState::from_target_state(&config.target_state)
            } else if self.filter_by_runtime(resource, config) {
                // Enforce max runtime for a resource if max_run_time is provided
                debug!(
                    resource = resource.id.as_str(),
                    "Resource exceeded max runtime."
                );
                EnforcementState::from_target_state(&config.target_state)
            } else if self.filter_by_idle_rules(resource, cw_client).await {
                // Enforce Idle rules
                debug!(resource = resource.id.as_str(), "Resource is idle.");
                EnforcementState::from_target_state(&config.target_state)
            } else if self.filter_by_naming_prefix(resource, config) {
                debug!(
                    resource = resource.id.as_str(),
                    "Resource naming prefix not met."
                );
                EnforcementState::from_target_state(&config.target_state)
            } else {
                if let Some(additional_filters) =
                    self.additional_filters(resource, config).await.take()
                {
                    if additional_filters {
                        {
                            // Apply any additional filters that are implemented by
                            // Resource clients.
                            debug!(
                                resource = resource.id.as_str(),
                                "Resource does not meet additional filters."
                            );
                            return EnforcementState::from_target_state(&config.target_state);
                        }
                    }
                }

                EnforcementState::Skip
            }
        } else {
            resource.enforcement_state
        }
    }

    async fn cleanup(&self, resource: &Resource) -> Result<()> {
        match resource.enforcement_state {
            EnforcementState::Stop => self.stop(resource).await?,
            EnforcementState::Delete | EnforcementState::DeleteDependent => {
                self.delete(resource).await?
            }
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
