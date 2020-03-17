mod asg;
mod aurora;
mod cloudwatch;
mod ebs;
mod ec2;
mod ecs;
mod elb;
mod emr;
mod es;
mod glue;
mod rds;
mod redshift;
mod s3;
mod sagemaker;
mod sts;
mod util;

use crate::{
    aws::{
        asg::AsgService, aurora::AuroraService, cloudwatch::CwClient, ebs::EbsService,
        ec2::Ec2Service, ecs::EcsService, elb::ElbService, emr::EmrService, es::EsService,
        glue::GlueService, rds::RdsService, redshift::RedshiftService, s3::S3Service,
        sagemaker::SagemakerService,
    },
    config::Config,
    error::Error as AwsError,
    resource::Resource,
    service::{self, NukerService},
};
use rusoto_core::Region;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tracing::{error, trace};
use tracing_futures::Instrument;

type Result<T, E = AwsError> = std::result::Result<T, E>;

macro_rules! cleanup_resources {
    ($resource_type:expr, $resources:expr, $handles:expr, $service:expr, $region:expr) => {
        if $resources.get($resource_type).is_some() {
            let meta_resources = $resources.get($resource_type).unwrap().clone();

            $handles.push(tokio::spawn(async move {
                match $service
                    .cleanup(meta_resources)
                    .instrument(tracing::trace_span!(
                        $resource_type,
                        region = $region.as_str()
                    ))
                    .await
                {
                    Ok(()) => {}
                    Err(err) => error!(
                        "Error occurred cleaning up {} resources: {}",
                        $resource_type, err
                    ),
                }
            }));
        }
    };
}

macro_rules! scan_resources {
    ($resource_type:expr, $resources:expr, $handles:expr, $service:expr, $region:expr) => {
        $handles.push(tokio::spawn(async move {
            match $service
                .scan()
                .instrument(tracing::trace_span!(
                    $resource_type,
                    region = $region.as_str()
                ))
                .await
            {
                Ok(rs) => {
                    if !rs.is_empty() {
                        for r in rs {
                            $resources.lock().unwrap().push(r);
                        }
                    }
                }
                Err(err) => {
                    error!("Error occurred locating resources: {}", err);
                }
            }
        }));
    };
}

/// AWS Nuker for nuking resources in AWS.
pub struct AwsNuker {
    pub region: Region,
    services: Vec<Box<dyn NukerService>>,
    resources: Arc<Mutex<Vec<Resource>>>,
}

impl AwsNuker {
    pub fn new(
        profile_name: Option<String>,
        region: Region,
        config: &Config,
        dry_run: bool,
    ) -> Result<AwsNuker> {
        let mut services: Vec<Box<dyn NukerService>> = Vec::new();
        let cw_client = create_cw_client(&profile_name, &region, config)?;

        if config.ec2.enabled {
            services.push(Box::new(Ec2Service::new(
                profile_name.clone(),
                region.clone(),
                config.ec2.clone(),
                cw_client.clone(),
                dry_run,
            )?));
        }

        if config.rds.enabled {
            services.push(Box::new(RdsService::new(
                profile_name.clone(),
                region.clone(),
                config.rds.clone(),
                cw_client.clone(),
                dry_run,
            )?));
        }

        if config.aurora.enabled {
            services.push(Box::new(AuroraService::new(
                profile_name.clone(),
                region.clone(),
                config.aurora.clone(),
                cw_client.clone(),
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
                cw_client.clone(),
                dry_run,
            )?))
        }

        if config.ebs.enabled {
            services.push(Box::new(EbsService::new(
                profile_name.clone(),
                region.clone(),
                config.ebs.clone(),
                cw_client.clone(),
                dry_run,
            )?))
        }

        if config.emr.enabled {
            services.push(Box::new(EmrService::new(
                profile_name.clone(),
                region.clone(),
                config.emr.clone(),
                cw_client.clone(),
                dry_run,
            )?))
        }

        if config.glue.enabled {
            services.push(Box::new(GlueService::new(
                profile_name.clone(),
                region.clone(),
                config.glue.clone(),
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

        if config.es.enabled {
            services.push(Box::new(EsService::new(
                profile_name.clone(),
                region.clone(),
                config.es.clone(),
                cw_client.clone(),
                dry_run,
            )?))
        }

        if config.elb.enabled {
            services.push(Box::new(ElbService::new(
                profile_name.clone(),
                region.clone(),
                config.elb.clone(),
                cw_client.clone(),
                dry_run,
            )?))
        }

        if config.asg.enabled {
            services.push(Box::new(AsgService::new(
                profile_name.clone(),
                region.clone(),
                config.asg.clone(),
                dry_run,
            )?))
        }

        if config.ecs.enabled {
            services.push(Box::new(EcsService::new(
                profile_name.clone(),
                region.clone(),
                config.ecs.clone(),
                cw_client.clone(),
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
        trace!("Init locate_resources");

        let mut handles = Vec::new();

        for service in &self.services {
            let service = dyn_clone::clone_box(&*service);
            let resources = self.resources.clone();
            let ref_client = service.as_any();
            let region = self.region.name().to_string();

            if ref_client.is::<Ec2Service>() {
                scan_resources!(service::EC2_TYPE, resources, handles, service, region);
            } else if ref_client.is::<EbsService>() {
                scan_resources!(service::EBS_TYPE, resources, handles, service, region);
            } else if ref_client.is::<RdsService>() {
                scan_resources!(service::RDS_TYPE, resources, handles, service, region);
            } else if ref_client.is::<AuroraService>() {
                scan_resources!(service::AURORA_TYPE, resources, handles, service, region);
            } else if ref_client.is::<RedshiftService>() {
                scan_resources!(service::REDSHIFT_TYPE, resources, handles, service, region);
            } else if ref_client.is::<EmrService>() {
                scan_resources!(service::EMR_TYPE, resources, handles, service, region);
            } else if ref_client.is::<GlueService>() {
                scan_resources!(service::GLUE_TYPE, resources, handles, service, region);
            } else if ref_client.is::<SagemakerService>() {
                scan_resources!(service::SAGEMAKER_TYPE, resources, handles, service, region);
            } else if ref_client.is::<S3Service>() {
                scan_resources!(service::S3_TYPE, resources, handles, service, region);
            } else if ref_client.is::<EsService>() {
                scan_resources!(service::ES_TYPE, resources, handles, service, region);
            } else if ref_client.is::<ElbService>() {
                scan_resources!(service::ELB_TYPE, resources, handles, service, region);
            } else if ref_client.is::<AsgService>() {
                scan_resources!(service::ASG_TYPE, resources, handles, service, region);
            } else if ref_client.is::<EcsService>() {
                scan_resources!(service::ECS_TYPE, resources, handles, service, region);
            }
        }

        futures::future::join_all(handles).await;
    }

    pub fn print_resources(&self) {
        for resource in self.resources.lock().unwrap().iter() {
            println!("{}", resource);
        }
    }

    pub async fn cleanup_resources(&self) -> Result<()> {
        trace!("Init cleanup resources");
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
            let region = self.region.name().to_string();

            if ref_client.is::<Ec2Service>() {
                cleanup_resources!(service::EC2_TYPE, resources, handles, service, region);
            } else if ref_client.is::<EbsService>() {
                cleanup_resources!(service::EBS_TYPE, resources, handles, service, region);
            } else if ref_client.is::<RdsService>() {
                cleanup_resources!(service::RDS_TYPE, resources, handles, service, region);
            } else if ref_client.is::<AuroraService>() {
                cleanup_resources!(service::AURORA_TYPE, resources, handles, service, region);
            } else if ref_client.is::<RedshiftService>() {
                cleanup_resources!(service::REDSHIFT_TYPE, resources, handles, service, region);
            } else if ref_client.is::<EmrService>() {
                cleanup_resources!(service::EMR_TYPE, resources, handles, service, region);
            } else if ref_client.is::<GlueService>() {
                cleanup_resources!(service::GLUE_TYPE, resources, handles, service, region);
            } else if ref_client.is::<SagemakerService>() {
                cleanup_resources!(service::SAGEMAKER_TYPE, resources, handles, service, region);
            } else if ref_client.is::<S3Service>() {
                cleanup_resources!(service::S3_TYPE, resources, handles, service, region);
            } else if ref_client.is::<EsService>() {
                cleanup_resources!(service::ES_TYPE, resources, handles, service, region);
            } else if ref_client.is::<ElbService>() {
                cleanup_resources!(service::ELB_TYPE, resources, handles, service, region);
            } else if ref_client.is::<AsgService>() {
                cleanup_resources!(service::ASG_TYPE, resources, handles, service, region);
            } else if ref_client.is::<EcsService>() {
                cleanup_resources!(service::ECS_TYPE, resources, handles, service, region);
            }
        }

        futures::future::join_all(handles).await;

        Ok(())
    }
}

fn create_cw_client(
    profile_name: &Option<String>,
    region: &Region,
    config: &Config,
) -> Result<Arc<Box<CwClient>>> {
    let cw_client: rusoto_cloudwatch::CloudWatchClient = if let Some(profile) = profile_name {
        let mut pp = rusoto_credential::ProfileProvider::new()?;
        pp.set_profile(profile);

        rusoto_cloudwatch::CloudWatchClient::new_with(
            rusoto_core::HttpClient::new()?,
            pp,
            region.to_owned(),
        )
    } else {
        rusoto_cloudwatch::CloudWatchClient::new(region.to_owned())
    };

    Ok(Arc::new(Box::new(CwClient {
        client: cw_client,
        ec2_idle_rules: config.ec2.idle_rules.clone(),
        ebs_idle_rules: config.ebs.idle_rules.clone(),
        elb_alb_idle_rules: config.elb.alb_idle_rules.clone(),
        elb_nlb_idle_rules: config.elb.nlb_idle_rules.clone(),
        rds_idle_rules: config.rds.idle_rules.clone(),
        aurora_idle_rules: config.aurora.idle_rules.clone(),
        redshift_idle_rules: config.redshift.idle_rules.clone(),
        emr_idle_rules: config.emr.idle_rules.clone(),
        es_idle_rules: config.es.idle_rules.clone(),
        ecs_idle_rules: config.ecs.idle_rules.clone(),
    })))
}
