mod aurora;
mod ce;
mod cloudwatch;
mod ebs;
mod ec2;
mod emr;
mod glue;
mod rds;
mod redshift;
mod s3;
mod sagemaker;
mod sts;
mod util;

use crate::aws::sagemaker::SagemakerNukeClient;
use crate::{
    aws::ebs::EbsNukeClient,
    config::Config,
    error::Error as AwsError,
    service::{NukeService, Resource},
};
use aurora::AuroraNukeClient;
use ce::CeClient;
use ec2::Ec2NukeClient;
use emr::EmrNukeClient;
use glue::GlueNukeClient;
use log::error;
use rds::RdsNukeClient;
use redshift::RedshiftNukeClient;
use rusoto_core::Region;
use s3::S3NukeClient;
use sts::StsNukeClient;

type Result<T, E = AwsError> = std::result::Result<T, E>;

pub struct AwsClient {
    pub region: Region,
    clients: Vec<Box<dyn NukeService>>,
    ce_client: Option<CeClient>,
    profile_name: Option<String>,
}

impl AwsClient {
    pub fn new(
        profile_name: Option<&str>,
        region: Region,
        config: &Config,
        dry_run: bool,
    ) -> Result<AwsClient> {
        let mut clients: Vec<Box<dyn NukeService>> = Vec::new();
        let sts_client = StsNukeClient::new(profile_name, region.clone())?;

        if config.ec2.enabled {
            clients.push(Box::new(Ec2NukeClient::new(
                profile_name,
                region.clone(),
                config.ec2.clone(),
                dry_run,
            )?));
        }

        if config.rds.enabled {
            clients.push(Box::new(RdsNukeClient::new(
                profile_name,
                region.clone(),
                config.rds.clone(),
                dry_run,
            )?));
        }

        if config.aurora.enabled {
            clients.push(Box::new(AuroraNukeClient::new(
                profile_name,
                region.clone(),
                config.aurora.clone(),
                dry_run,
            )?))
        }

        if config.s3.enabled {
            clients.push(Box::new(S3NukeClient::new(
                profile_name,
                region.clone(),
                config.s3.clone(),
                dry_run,
            )?))
        }

        if config.redshift.enabled {
            clients.push(Box::new(RedshiftNukeClient::new(
                profile_name,
                region.clone(),
                config.redshift.clone(),
                dry_run,
            )?))
        }

        if config.ebs.enabled {
            clients.push(Box::new(EbsNukeClient::new(
                profile_name,
                region.clone(),
                config.ebs.clone(),
                dry_run,
            )?))
        }

        if config.emr.enabled {
            clients.push(Box::new(EmrNukeClient::new(
                profile_name,
                region.clone(),
                config.emr.clone(),
                dry_run,
            )?))
        }

        if config.glue.enabled {
            clients.push(Box::new(GlueNukeClient::new(
                profile_name,
                region.clone(),
                config.glue.clone(),
                sts_client.get_account_number()?,
                dry_run,
            )?))
        }

        if config.sagemaker.enabled {
            clients.push(Box::new(SagemakerNukeClient::new(
                profile_name,
                region.clone(),
                config.sagemaker.clone(),
                dry_run,
            )?))
        }

        let ce_client = if config.print_usage {
            Some(CeClient::new(profile_name, config.usage_days)?)
        } else {
            None
        };

        Ok(AwsClient {
            region,
            clients,
            ce_client,
            profile_name: profile_name.map(|s| s.into()),
        })
    }

    pub fn locate_resources(&self) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for client in &self.clients {
            match client.scan() {
                Ok(rs) => {
                    if !rs.is_empty() {
                        for r in rs {
                            resources.push(r);
                        }
                    }
                }
                Err(err) => {
                    error!(
                        "Error occurred locating resources in region: '{:?}' using profile: '{:?}'. {}",
                        self.region, self.profile_name, err
                    );
                }
            }
        }

        Ok(resources)
    }

    pub fn cleanup_resources(&self, resources: &[Resource]) -> Result<()> {
        let ec2_resources: Vec<&Resource> = resources
            .iter()
            .filter(|r| r.resource_type.is_ec2())
            .collect();
        let ebs_resources: Vec<&Resource> = resources
            .iter()
            .filter(|r| r.resource_type.is_ebs())
            .collect();
        let rds_resources: Vec<&Resource> = resources
            .iter()
            .filter(|r| r.resource_type.is_rds())
            .collect();
        let aurora_resources: Vec<&Resource> = resources
            .iter()
            .filter(|r| r.resource_type.is_aurora())
            .collect();
        let s3_resources: Vec<&Resource> = resources
            .iter()
            .filter(|r| r.resource_type.is_s3())
            .collect();
        let redshift_resources: Vec<&Resource> = resources
            .iter()
            .filter(|r| r.resource_type.is_redshift())
            .collect();
        let emr_resources: Vec<&Resource> = resources
            .iter()
            .filter(|r| r.resource_type.is_emr())
            .collect();
        let glue_resources: Vec<&Resource> = resources
            .iter()
            .filter(|r| r.resource_type.is_glue())
            .collect();
        let sagemaker_resources: Vec<&Resource> = resources
            .iter()
            .filter(|r| r.resource_type.is_sagemaker())
            .collect();

        for client in &self.clients {
            let ref_client = client.as_any();

            if ref_client.is::<Ec2NukeClient>() {
                client.cleanup(&ec2_resources)?;
            } else if ref_client.is::<EbsNukeClient>() {
                client.cleanup(&ebs_resources)?;
            } else if ref_client.is::<RdsNukeClient>() {
                client.cleanup(&rds_resources)?;
            } else if ref_client.is::<AuroraNukeClient>() {
                client.cleanup(&aurora_resources)?;
            } else if ref_client.is::<S3NukeClient>() {
                client.cleanup(&s3_resources)?;
            } else if ref_client.is::<RedshiftNukeClient>() {
                client.cleanup(&redshift_resources)?;
            } else if ref_client.is::<EmrNukeClient>() {
                client.cleanup(&emr_resources)?;
            } else if ref_client.is::<GlueNukeClient>() {
                client.cleanup(&glue_resources)?;
            } else if ref_client.is::<SagemakerNukeClient>() {
                client.cleanup(&sagemaker_resources)?;
            }
        }

        Ok(())
    }

    pub fn print_usage(&self) -> Result<()> {
        if let Some(ce_client) = &self.ce_client {
            ce_client.get_usage()?;
        }

        Ok(())
    }
}
