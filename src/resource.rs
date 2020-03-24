use crate::{config::TargetState, service::*};
use colored::*;
use rusoto_core::Region;
use std::fmt;

#[derive(Display, Debug, Clone)]
pub enum ResourceType {
    Ec2Instance,
    Ec2Interface,
    Ec2Address,
    Ec2Sg,
    EbsVolume,
    EbsSnapshot,
    RDS,
    Aurora,
    S3Bucket,
    Redshift,
    EmrCluster,
    GlueDevEndpoint,
    SagemakerNotebook,
    EsDomain,
    ElbAlb,
    ElbNlb,
    Asg,
    EcsCluster,
    Vpc,
    VpcIgw,
    VpcSubnet,
    VpcRt,
    VpcNacl,
    VpcPeerConn,
    VpcEndpoint,
    VpcNatGw,
    VpcVpnGw,
    Root, // for tracking DAG dependencies
}

impl ResourceType {
    pub fn name(&self) -> &str {
        match *self {
            ResourceType::Ec2Instance
            | ResourceType::Ec2Interface
            | ResourceType::Ec2Address
            | ResourceType::Ec2Sg => EC2_TYPE,
            ResourceType::EbsVolume | ResourceType::EbsSnapshot => EBS_TYPE,
            ResourceType::RDS => RDS_TYPE,
            ResourceType::Aurora => AURORA_TYPE,
            ResourceType::S3Bucket => S3_TYPE,
            ResourceType::Redshift => REDSHIFT_TYPE,
            ResourceType::EmrCluster => EMR_TYPE,
            ResourceType::GlueDevEndpoint => GLUE_TYPE,
            ResourceType::SagemakerNotebook => SAGEMAKER_TYPE,
            ResourceType::EsDomain => ES_TYPE,
            ResourceType::ElbAlb => ELB_TYPE,
            ResourceType::ElbNlb => ELB_TYPE,
            ResourceType::Asg => ASG_TYPE,
            ResourceType::EcsCluster => ECS_TYPE,
            ResourceType::Vpc
            | ResourceType::VpcIgw
            | ResourceType::VpcSubnet
            | ResourceType::VpcRt
            | ResourceType::VpcNacl
            | ResourceType::VpcPeerConn
            | ResourceType::VpcEndpoint
            | ResourceType::VpcNatGw
            | ResourceType::VpcVpnGw => VPC_TYPE,
            ResourceType::Root => "root",
        }
    }

    pub fn is_ec2(&self) -> bool {
        match *self {
            ResourceType::Ec2Instance
            | ResourceType::Ec2Interface
            | ResourceType::Ec2Address
            | ResourceType::Ec2Sg => true,
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

    pub fn is_sagemaker(&self) -> bool {
        match *self {
            ResourceType::SagemakerNotebook => true,
            _ => false,
        }
    }

    pub fn is_es(&self) -> bool {
        match *self {
            ResourceType::EsDomain => true,
            _ => false,
        }
    }

    pub fn is_elb(&self) -> bool {
        match *self {
            ResourceType::ElbAlb | ResourceType::ElbNlb => true,
            _ => false,
        }
    }

    pub fn is_asg(&self) -> bool {
        match *self {
            ResourceType::Asg => true,
            _ => false,
        }
    }

    pub fn is_ecs(&self) -> bool {
        match *self {
            ResourceType::EcsCluster => true,
            _ => false,
        }
    }

    pub fn is_vpc(&self) -> bool {
        match *self {
            ResourceType::Vpc
            | ResourceType::VpcIgw
            | ResourceType::VpcSubnet
            | ResourceType::VpcRt
            | ResourceType::VpcNacl
            | ResourceType::VpcPeerConn
            | ResourceType::VpcEndpoint
            | ResourceType::VpcNatGw
            | ResourceType::VpcVpnGw => true,
            _ => false,
        }
    }

    pub fn is_root(&self) -> bool {
        match *self {
            ResourceType::Root => true,
            _ => false,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum EnforcementState {
    Stop,
    Delete,
    Skip,
    SkipConfig,
    SkipStopped,
    SkipUnknownState,
}

impl EnforcementState {
    pub fn name(&self) -> colored::ColoredString {
        match *self {
            EnforcementState::Stop => "would be stopped".blue().bold(),
            EnforcementState::Delete => "would be removed".blue().bold(),
            EnforcementState::Skip => "skipped because of rules".yellow().bold(),
            EnforcementState::SkipConfig => "skipped because of config".yellow().bold(),
            EnforcementState::SkipStopped => "skipped as resource is not running".yellow().bold(),
            EnforcementState::SkipUnknownState => {
                "skipped as resource state is unknown".yellow().bold()
            }
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

#[derive(Debug, Clone)]
pub struct Resource {
    pub id: String,
    pub arn: Option<String>,
    pub resource_type: ResourceType,
    pub region: Region,
    pub tags: Option<Vec<NTag>>,
    pub state: Option<String>,
    pub enforcement_state: EnforcementState,
    pub dependencies: Option<Vec<Resource>>,
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
