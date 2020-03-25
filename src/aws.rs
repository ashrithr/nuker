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
mod vpc;

use crate::{
    aws::{
        asg::AsgService, aurora::AuroraService, cloudwatch::CwClient, ebs::EbsService,
        ec2::Ec2Service, ecs::EcsService, elb::ElbService, emr::EmrService, es::EsService,
        glue::GlueService, rds::RdsService, redshift::RedshiftService, s3::S3Service,
        sagemaker::SagemakerService, vpc::VpcService,
    },
    config::Config,
    graph::Dag,
    resource::Resource,
    service::{self, NukerService},
    Result,
};
use rusoto_core::Region;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tracing::{error, trace};
use tracing_futures::Instrument;

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
    pub config: Config,
    services_map: HashMap<String, Box<dyn NukerService>>,
    resources: Arc<Mutex<Vec<Resource>>>,
    dag: Dag,
}

impl AwsNuker {
    pub fn new(
        profile_name: Option<String>,
        region: Region,
        config: Config,
        dry_run: bool,
    ) -> Result<AwsNuker> {
        let mut services_map: HashMap<String, Box<dyn NukerService>> = HashMap::new();
        let cw_client = create_cw_client(&profile_name, &region, &config)?;

        services_map.insert(
            service::EC2_TYPE.to_string(),
            Box::new(Ec2Service::new(
                profile_name.clone(),
                region.clone(),
                config.ec2.clone(),
                cw_client.clone(),
                dry_run,
            )?),
        );

        services_map.insert(
            service::RDS_TYPE.to_string(),
            Box::new(RdsService::new(
                profile_name.clone(),
                region.clone(),
                config.rds.clone(),
                cw_client.clone(),
                dry_run,
            )?),
        );

        services_map.insert(
            service::AURORA_TYPE.to_string(),
            Box::new(AuroraService::new(
                profile_name.clone(),
                region.clone(),
                config.aurora.clone(),
                cw_client.clone(),
                dry_run,
            )?),
        );

        services_map.insert(
            service::S3_TYPE.to_string(),
            Box::new(S3Service::new(
                profile_name.clone(),
                region.clone(),
                config.s3.clone(),
                dry_run,
            )?),
        );

        services_map.insert(
            service::REDSHIFT_TYPE.to_string(),
            Box::new(RedshiftService::new(
                profile_name.clone(),
                region.clone(),
                config.redshift.clone(),
                cw_client.clone(),
                dry_run,
            )?),
        );

        services_map.insert(
            service::EBS_TYPE.to_string(),
            Box::new(EbsService::new(
                profile_name.clone(),
                region.clone(),
                config.ebs.clone(),
                cw_client.clone(),
                dry_run,
            )?),
        );

        services_map.insert(
            service::EMR_TYPE.to_string(),
            Box::new(EmrService::new(
                profile_name.clone(),
                region.clone(),
                config.emr.clone(),
                cw_client.clone(),
                dry_run,
            )?),
        );

        services_map.insert(
            service::GLUE_TYPE.to_string(),
            Box::new(GlueService::new(
                profile_name.clone(),
                region.clone(),
                config.glue.clone(),
                dry_run,
            )?),
        );

        services_map.insert(
            service::SAGEMAKER_TYPE.to_string(),
            Box::new(SagemakerService::new(
                profile_name.clone(),
                region.clone(),
                config.sagemaker.clone(),
                dry_run,
            )?),
        );

        services_map.insert(
            service::ES_TYPE.to_string(),
            Box::new(EsService::new(
                profile_name.clone(),
                region.clone(),
                config.es.clone(),
                cw_client.clone(),
                dry_run,
            )?),
        );

        services_map.insert(
            service::ELB_TYPE.to_string(),
            Box::new(ElbService::new(
                profile_name.clone(),
                region.clone(),
                config.elb.clone(),
                cw_client.clone(),
                dry_run,
            )?),
        );

        services_map.insert(
            service::ASG_TYPE.to_string(),
            Box::new(AsgService::new(
                profile_name.clone(),
                region.clone(),
                config.asg.clone(),
                dry_run,
            )?),
        );

        services_map.insert(
            service::ECS_TYPE.to_string(),
            Box::new(EcsService::new(
                profile_name.clone(),
                region.clone(),
                config.ecs.clone(),
                cw_client.clone(),
                dry_run,
            )?),
        );

        services_map.insert(
            service::VPC_TYPE.to_string(),
            Box::new(VpcService::new(
                profile_name.clone(),
                region.clone(),
                config.vpc.clone(),
                dry_run,
            )?),
        );

        Ok(AwsNuker {
            region,
            config,
            services_map,
            resources: Arc::new(Mutex::new(Vec::new())),
            dag: Dag::new(),
        })
    }

    pub async fn locate_resources(&mut self) {
        let mut handles = Vec::new();

        for (_name, service) in &self.services_map {
            let service = dyn_clone::clone_box(&*service);
            let resources = self.resources.clone();
            let ref_client = service.as_any();
            let region = self.region.name().to_string();

            if ref_client.is::<Ec2Service>() && self.config.ec2.enabled {
                scan_resources!(service::EC2_TYPE, resources, handles, service, region);
            } else if ref_client.is::<EbsService>() && self.config.ebs.enabled {
                scan_resources!(service::EBS_TYPE, resources, handles, service, region);
            } else if ref_client.is::<RdsService>() && self.config.rds.enabled {
                scan_resources!(service::RDS_TYPE, resources, handles, service, region);
            } else if ref_client.is::<AuroraService>() && self.config.aurora.enabled {
                scan_resources!(service::AURORA_TYPE, resources, handles, service, region);
            } else if ref_client.is::<RedshiftService>() && self.config.redshift.enabled {
                scan_resources!(service::REDSHIFT_TYPE, resources, handles, service, region);
            } else if ref_client.is::<EmrService>() && self.config.emr.enabled {
                scan_resources!(service::EMR_TYPE, resources, handles, service, region);
            } else if ref_client.is::<GlueService>() && self.config.glue.enabled {
                scan_resources!(service::GLUE_TYPE, resources, handles, service, region);
            } else if ref_client.is::<SagemakerService>() && self.config.sagemaker.enabled {
                scan_resources!(service::SAGEMAKER_TYPE, resources, handles, service, region);
            } else if ref_client.is::<S3Service>() && self.config.s3.enabled {
                scan_resources!(service::S3_TYPE, resources, handles, service, region);
            } else if ref_client.is::<EsService>() && self.config.es.enabled {
                scan_resources!(service::ES_TYPE, resources, handles, service, region);
            } else if ref_client.is::<ElbService>() && self.config.elb.enabled {
                scan_resources!(service::ELB_TYPE, resources, handles, service, region);
            } else if ref_client.is::<AsgService>() && self.config.asg.enabled {
                scan_resources!(service::ASG_TYPE, resources, handles, service, region);
            } else if ref_client.is::<EcsService>() && self.config.ecs.enabled {
                scan_resources!(service::ECS_TYPE, resources, handles, service, region);
            } else if ref_client.is::<VpcService>() && self.config.vpc.enabled {
                scan_resources!(service::VPC_TYPE, resources, handles, service, region);
            }
        }

        futures::future::join_all(handles).await;
    }

    pub fn print_resources(&self) {
        for resource in self.resources.lock().unwrap().iter() {
            println!("{}", resource);
        }
    }

    fn order_deps(&mut self) -> Result<Vec<Resource>> {
        let resources_guard = self.resources.lock().unwrap();
        self.dag.build_graph(resources_guard.as_slice())?;
        self.dag.order_by_dependencies()
    }

    pub async fn cleanup_resources(&mut self) -> Result<()> {
        for resource in self.order_deps()? {
            self.services_map
                .get(&resource.resource_type.name().to_string())
                .unwrap()
                .cleanup(&resource)
                .await?;
        }
        trace!("Done cleaning up resources");

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
