// mod asg;
// mod aurora;
mod cloudwatch;
// mod ebs;
// mod ec2;
// mod ecs;
// mod elb;
// mod emr;
// mod es;
// mod glue;
// mod rds;
// mod redshift;
// mod s3;
// mod sagemaker;
// mod sts;
// mod util;
// mod vpc;
mod ec2_instance;

pub use cloudwatch::CwClient;

use crate::{
    // aws::{
    //     asg::AsgService, aurora::AuroraService, cloudwatch::CwClient, ebs::EbsService,
    //     ec2::Ec2Service, ecs::EcsService, elb::ElbService, emr::EmrService, es::EsService,
    //     glue::GlueService, rds::RdsService, redshift::RedshiftService, s3::S3Service,
    //     sagemaker::SagemakerService, vpc::VpcService,
    // },
    aws::ec2_instance::Ec2Instance,
    client::NukerClient,
    config::Config,
    graph::Dag,
    resource::{Resource, ResourceType},
    scan_resources,
    service::{self, NukerService},
    Result,
};
use rusoto_core::{Client, HttpClient, Region};
use rusoto_credential::{ChainProvider, ProfileProvider};
use std::time::Duration;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tracing::trace;
use tracing_futures::Instrument;

type ClientType = ResourceType;

/// AWS Nuker for nuking resources in AWS.
pub struct AwsNuker {
    pub region: Region,
    pub config: Config,
    // services_map: HashMap<String, Box<dyn NukerService>>,
    clients: HashMap<ClientType, Box<dyn NukerClient>>,
    resources: Arc<Mutex<Vec<Resource>>>,
    dag: Dag,
}

impl AwsNuker {
    pub fn new(
        profile: Option<String>,
        region: Region,
        mut config: Config,
        dry_run: bool,
    ) -> Result<AwsNuker> {
        // let mut services_map: HashMap<String, Box<dyn NukerService>> = HashMap::new();
        let client = Client::new_with(credentials_provider(&profile)?, HttpClient::new()?);
        let mut clients: HashMap<ClientType, Box<dyn NukerClient>> = HashMap::new();
        let cw_client = create_cw_client(&profile, &region, &mut config)?;

        /*
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
        */

        if config.ec2.is_some() {
            clients.insert(
                ClientType::Ec2Instance,
                Box::new(Ec2Instance::new(
                    &client,
                    &region,
                    std::mem::replace(&mut config.ec2, None).unwrap(),
                    cw_client,
                    dry_run,
                )),
            );
        }

        Ok(AwsNuker {
            region,
            config,
            clients,
            resources: Arc::new(Mutex::new(Vec::new())),
            dag: Dag::new(),
        })
    }

    pub async fn locate_resources(&mut self) {
        // let mut handles = Vec::new();

        /*
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
        */

        // futures::future::join_all(handles).await;

        unimplemented!()
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
        /*
        for resource in self.order_deps()? {
            self.clients
                .get(&resource.resource_type)
                .unwrap()
                .cleanup(&resource)
                .await?;
        }
        trace!("Done cleaning up resources");
        */

        Ok(())
    }
}

pub fn credentials_provider(profile: &Option<String>) -> Result<ChainProvider> {
    let profile_provider = match profile {
        Some(profile) => {
            let mut p = ProfileProvider::new()?;
            p.set_profile(profile);
            p
        }
        None => ProfileProvider::new()?,
    };

    let mut provider = ChainProvider::with_profile_provider(profile_provider);
    provider.set_timeout(Duration::from_millis(250));
    Ok(provider)
}

fn create_cw_client(
    profile: &Option<String>,
    region: &Region,
    config: &mut Config,
) -> Result<Arc<Box<CwClient>>> {
    let cw_client: rusoto_cloudwatch::CloudWatchClient =
        rusoto_cloudwatch::CloudWatchClient::new_with_client(
            Client::new_with(credentials_provider(profile)?, HttpClient::new()?),
            region.to_owned(),
        );

    Ok(Arc::new(Box::new(CwClient {
        client: cw_client,
        ec2_idle_rules: if let Some(ec2) = &mut config.ec2 {
            std::mem::replace(&mut ec2.idle_rules, None)
        } else {
            None
        },
        ebs_idle_rules: if let Some(ebs) = &mut config.ebs {
            std::mem::replace(&mut ebs.idle_rules, None)
        } else {
            None
        },
        elb_alb_idle_rules: if let Some(elb_alb) = &mut config.elb {
            std::mem::replace(&mut elb_alb.alb_idle_rules, None)
        } else {
            None
        },
        elb_nlb_idle_rules: if let Some(elb_nlb) = &mut config.elb {
            std::mem::replace(&mut elb_nlb.nlb_idle_rules, None)
        } else {
            None
        },
        rds_idle_rules: if let Some(rds) = &mut config.rds {
            std::mem::replace(&mut rds.idle_rules, None)
        } else {
            None
        },
        aurora_idle_rules: if let Some(aurora) = &mut config.aurora {
            std::mem::replace(&mut aurora.idle_rules, None)
        } else {
            None
        },
        redshift_idle_rules: if let Some(redshift) = &mut config.redshift {
            std::mem::replace(&mut redshift.idle_rules, None)
        } else {
            None
        },
        emr_idle_rules: if let Some(emr) = &mut config.emr {
            std::mem::replace(&mut emr.idle_rules, None)
        } else {
            None
        },
        es_idle_rules: if let Some(es) = &mut config.es {
            std::mem::replace(&mut es.idle_rules, None)
        } else {
            None
        },
        ecs_idle_rules: if let Some(ecs) = &mut config.ecs {
            std::mem::replace(&mut ecs.idle_rules, None)
        } else {
            None
        },
    })))
}
