mod aurora;
// mod ce;
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

// use crate::aws::sagemaker::SagemakerNukeClient;
use crate::{
    aws::{
        aurora::AuroraService, ebs::EbsService, ec2::Ec2Service, emr::EmrService,
        glue::GlueService, rds::RdsService, redshift::RedshiftService, s3::S3Service,
        sagemaker::SagemakerService, sts::StsService,
    },
    config::Config,
    error::Error as AwsError,
    resource::{self, Resource},
    service::NukerService,
};
use log::error;
use rusoto_core::Region;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type Result<T, E = AwsError> = std::result::Result<T, E>;

/// AWS Nuker for nuking resources in AWS.
pub struct AwsNuker {
    pub region: Region,
    services: Vec<Box<dyn NukerService>>,
    resources: Arc<Mutex<Vec<Resource>>>,
}

impl AwsNuker {
    pub async fn new(
        profile_name: Option<String>,
        region: Region,
        config: &Config,
        dry_run: bool,
    ) -> Result<AwsNuker> {
        let mut services: Vec<Box<dyn NukerService>> = Vec::new();
        let sts_service = StsService::new(profile_name.clone(), region.clone())?;

        if config.ec2.enabled {
            services.push(Box::new(Ec2Service::new(
                profile_name.clone(),
                region.clone(),
                config.ec2.clone(),
                dry_run,
            )?));
        }

        if config.rds.enabled {
            services.push(Box::new(RdsService::new(
                profile_name.clone(),
                region.clone(),
                config.rds.clone(),
                dry_run,
            )?));
        }

        if config.aurora.enabled {
            services.push(Box::new(AuroraService::new(
                profile_name.clone(),
                region.clone(),
                config.aurora.clone(),
                dry_run,
            )?))
        }

        if config.s3.enabled {
            services.push(Box::new(S3Service::new(
                profile_name.clone(),
                region.clone(),
                config.s3.clone(),
                dry_run,
            )?))
        }

        if config.redshift.enabled {
            services.push(Box::new(RedshiftService::new(
                profile_name.clone(),
                region.clone(),
                config.redshift.clone(),
                dry_run,
            )?))
        }

        if config.ebs.enabled {
            services.push(Box::new(EbsService::new(
                profile_name.clone(),
                region.clone(),
                config.ebs.clone(),
                dry_run,
            )?))
        }

        if config.emr.enabled {
            services.push(Box::new(EmrService::new(
                profile_name.clone(),
                region.clone(),
                config.emr.clone(),
                dry_run,
            )?))
        }

        if config.glue.enabled {
            services.push(Box::new(GlueService::new(
                profile_name.clone(),
                region.clone(),
                config.glue.clone(),
                sts_service.get_account_number().await?,
                dry_run,
            )?))
        }

        if config.sagemaker.enabled {
            services.push(Box::new(SagemakerService::new(
                profile_name.clone(),
                region.clone(),
                config.sagemaker.clone(),
                dry_run,
            )?))
        }

        Ok(AwsNuker {
            region,
            services,
            resources: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub async fn locate_resources(&mut self) {
        let mut handles = Vec::new();

        for service in &self.services {
            let service = dyn_clone::clone_box(&*service);
            let resources = self.resources.clone();

            handles.push(tokio::spawn(async move {
                match service.scan().await {
                    Ok(rs) => {
                        if !rs.is_empty() {
                            for r in rs {
                                resources.lock().unwrap().push(r);
                            }
                        }
                    }
                    Err(err) => {
                        error!("Error occurred locating resources: {}", err);
                    }
                }
            }));
        }

        futures::future::join_all(handles).await;
    }

    pub fn print_resources(&self) {
        for resource in self.resources.lock().unwrap().iter() {
            println!("{}", resource);
        }
    }

    pub async fn cleanup_resources(&self) -> Result<()> {
        let mut handles = Vec::new();
        let mut resources: HashMap<String, Vec<Resource>> = HashMap::new();

        for resource in self.resources.lock().unwrap().iter() {
            let key = resource.resource_type.name().to_owned();
            if !resources.contains_key(&key) {
                resources.insert(key.clone(), vec![]);
            }
            resources.get_mut(&key).unwrap().push(resource.clone());
        }

        for service in &self.services {
            let service = dyn_clone::clone_box(&*service);
            let ref_client = service.as_any();

            if ref_client.is::<Ec2Service>() {
                if resources.get(resource::EC2_TYPE).is_some() {
                    let ec2_resources = resources.get(resource::EC2_TYPE).unwrap().clone();

                    handles.push(tokio::spawn(async move {
                        match service.cleanup(ec2_resources).await {
                            Ok(()) => {}
                            Err(err) => error!("Error occurred cleaning up EC2 resources: {}", err),
                        }
                    }));
                }
            } else if ref_client.is::<EbsService>() {
                if resources.get(resource::EBS_TYPE).is_some() {
                    let ebs_resources = resources.get(resource::EBS_TYPE).unwrap().clone();

                    handles.push(tokio::spawn(async move {
                        match service.cleanup(ebs_resources).await {
                            Ok(()) => {}
                            Err(err) => error!("Error occurred cleaning up EBS resources: {}", err),
                        }
                    }));
                }
            } else if ref_client.is::<RdsService>() {
                if resources.get(resource::RDS_TYPE).is_some() {
                    let rds_resources = resources.get(resource::RDS_TYPE).unwrap().clone();

                    handles.push(tokio::spawn(async move {
                        match service.cleanup(rds_resources).await {
                            Ok(()) => {}
                            Err(err) => error!("Error occurred cleaning up RDS resources: {}", err),
                        }
                    }));
                }
            } else if ref_client.is::<AuroraService>() {
                if resources.get(resource::AURORA_TYPE).is_some() {
                    let aurora_resources = resources.get(resource::AURORA_TYPE).unwrap().clone();

                    handles.push(tokio::spawn(async move {
                        match service.cleanup(aurora_resources).await {
                            Ok(()) => {}
                            Err(err) => {
                                error!("Error occurred cleaning up Aurora resources: {}", err)
                            }
                        }
                    }));
                }
            } else if ref_client.is::<RedshiftService>() {
                if resources.get(resource::REDSHIFT_TYPE).is_some() {
                    let rs_resources = resources.get(resource::REDSHIFT_TYPE).unwrap().clone();

                    handles.push(tokio::spawn(async move {
                        match service.cleanup(rs_resources).await {
                            Ok(()) => {}
                            Err(err) => {
                                error!("Error occurred cleaning up Aurora resources: {}", err)
                            }
                        }
                    }));
                }
            } else if ref_client.is::<EmrService>() {
                if resources.get(resource::EMR_TYPE).is_some() {
                    let emr_resources = resources.get(resource::EMR_TYPE).unwrap().clone();

                    handles.push(tokio::spawn(async move {
                        match service.cleanup(emr_resources).await {
                            Ok(()) => {}
                            Err(err) => error!("Error occurred cleaning up EMR resources: {}", err),
                        }
                    }));
                }
            } else if ref_client.is::<GlueService>() {
                if resources.get(resource::GLUE_TYPE).is_some() {
                    let glue_resources = resources.get(resource::GLUE_TYPE).unwrap().clone();

                    handles.push(tokio::spawn(async move {
                        match service.cleanup(glue_resources).await {
                            Ok(()) => {}
                            Err(err) => {
                                error!("Error occurred cleaning up Glue resources: {}", err)
                            }
                        }
                    }));
                }
            } else if ref_client.is::<SagemakerService>() {
                if resources.get(resource::SAGEMAKER_TYPE).is_some() {
                    let sm_resources = resources.get(resource::SAGEMAKER_TYPE).unwrap().clone();

                    handles.push(tokio::spawn(async move {
                        match service.cleanup(sm_resources).await {
                            Ok(()) => {}
                            Err(err) => {
                                error!("Error occurred cleaning up Sagemaker resources: {}", err)
                            }
                        }
                    }));
                }
            } else if ref_client.is::<S3Service>() {
                if resources.get(resource::S3_TYPE).is_some() {
                    let s3_resources = resources.get(resource::S3_TYPE).unwrap().clone();

                    handles.push(tokio::spawn(async move {
                        match service.cleanup(s3_resources).await {
                            Ok(()) => {}
                            Err(err) => error!("Error occurred cleaning up S3 resources: {}", err),
                        }
                    }));
                }
            }
        }

        futures::future::join_all(handles).await;

        Ok(())
    }

    // pub fn print_usage(&self) -> Result<()> {
    //     if let Some(ce_client) = &self.ce_client {
    //         ce_client.get_usage()?;
    //     }

    //     Ok(())
    // }
}
