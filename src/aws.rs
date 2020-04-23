mod asg;
mod cloudwatch;
mod ebs_snapshot;
mod ebs_volume;
mod ec2_address;
mod ec2_eni;
mod ec2_igw;
mod ec2_instance;
mod ec2_nat_gw;
mod ec2_network_acl;
mod ec2_peer_conn;
mod ec2_rt;
mod ec2_sg;
mod ec2_subnet;
mod ec2_vpc;
mod ec2_vpc_endpoint;
mod ec2_vpn_gw;
mod ecs_cluster;
mod eks_cluster;
mod elb_alb;
mod elb_nlb;
mod emr_cluster;
mod es_domain;
mod glue_endpoint;
mod rds_cluster;
mod rds_instance;
mod rs_cluster;
mod s3_bucket;
mod sagemaker_notebook;
mod sts;

pub use cloudwatch::CwClient;

use crate::Event;
use crate::{
    aws::{
        asg::AsgClient, ebs_snapshot::EbsSnapshotClient, ebs_volume::EbsVolumeClient,
        ec2_address::Ec2AddressClient, ec2_eni::Ec2EniClient, ec2_igw::Ec2IgwClient,
        ec2_instance::Ec2InstanceClient, ec2_nat_gw::Ec2NatGWClient,
        ec2_network_acl::Ec2NetworkAclClient, ec2_peer_conn::Ec2PeerConnClient,
        ec2_rt::Ec2RtClient, ec2_sg::Ec2SgClient, ec2_subnet::Ec2SubnetClient,
        ec2_vpc::Ec2VpcClient, ec2_vpc_endpoint::Ec2VpcEndpointClient, ec2_vpn_gw::Ec2VpnGWClient,
        ecs_cluster::EcsClusterClient, eks_cluster::EksClusterClient, elb_alb::ElbAlbClient,
        elb_nlb::ElbNlbClient, emr_cluster::EmrClusterClient, es_domain::EsDomainClient,
        glue_endpoint::GlueEndpointClient, rds_cluster::RdsClusterClient,
        rds_instance::RdsInstanceClient, rs_cluster::RsClusterClient, s3_bucket::S3BucketClient,
        sagemaker_notebook::SagemakerNotebookClient, sts::StsService,
    },
    client::Client,
    client::NukerClient,
    config::Config,
    graph::{is_dag, Dag},
    resource::EnforcementState,
    Error, Result,
};
use rusoto_core::{Client as RClient, HttpClient, Region};
use rusoto_credential::{ChainProvider, ProfileProvider};
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::trace;

#[derive(Clone)]
pub struct ClientDetails {
    account_number: String,
    client: RClient,
    pub region: Region,
}

/// AWS Nuker for nuking resources in AWS.
pub struct AwsNuker {
    pub client_details: ClientDetails,
    config: Config,
    clients: HashMap<Client, Box<dyn NukerClient>>,
    cw_client: Arc<Box<CwClient>>,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    dag: Dag,
    dry_run: bool,
}

impl AwsNuker {
    pub async fn new(
        profile: Option<String>,
        region: Region,
        mut config: Config,
        excluded_clients: Vec<Client>,
        dry_run: bool,
    ) -> Result<AwsNuker> {
        let client = RClient::new_with(credentials_provider(&profile)?, HttpClient::new()?);
        let mut clients: HashMap<Client, Box<dyn NukerClient>> = HashMap::new();
        let sts_client = StsService::new(&client, &region)?;
        let cw_client = create_cw_client(&profile, &region, &mut config)?;

        let client_details = ClientDetails {
            account_number: sts_client.get_account_number().await?,
            region: region.clone(),
            client,
        };

        for client in Client::iter() {
            if !excluded_clients.contains(&client) {
                clients.insert(
                    client,
                    create_client(&client, &client_details, &config, dry_run).clone(),
                );
            }
        }

        let (tx, rx) = channel(100);

        Ok(AwsNuker {
            client_details,
            config,
            clients,
            cw_client,
            tx,
            rx,
            dag: Dag::new(),
            dry_run,
        })
    }

    /// Locates resources across all clients for a particular region
    pub async fn locate_resources(&mut self) {
        let mut handles = Vec::new();

        for (client_type, client) in &self.clients {
            let client = dyn_clone::clone_box(&*client);
            let tx = self.tx.clone();
            let client_type = client_type.clone();
            let cw_client = self.cw_client.clone();
            let config = self.config.get(&client_type).unwrap().clone();

            handles.push(tokio::spawn(async move {
                client.publish(tx, client_type, config, cw_client).await
            }));
        }

        futures::future::join_all(handles).await;
    }

    /// Builds a dependency graph of resources
    async fn build_dag(&mut self) -> Result<()> {
        let mut done: usize = 0;

        while let Some(r) = self.rx.recv().await {
            match r {
                Event::Resource(resource) => {
                    self.dag.add_node_to_dag(resource.clone());

                    if resource.enforcement_state == EnforcementState::Delete
                        || resource.enforcement_state == EnforcementState::DeleteDependent
                    {
                        // FIXME: This is redundant
                        if let Some(deps) = resource.dependencies {
                            for mut dep in deps {
                                if !self.clients.contains_key(&dep.type_) {
                                    dep.dependencies = self
                                        .clients
                                        .entry(dep.type_)
                                        // For handling clients required for dependent resources
                                        .or_insert(create_client(
                                            &resource.type_,
                                            &self.client_details,
                                            &self.config,
                                            self.dry_run,
                                        ))
                                        .dependencies(&dep)
                                        .await;

                                    done = done + 1; // newly created client does not send Shutdown event
                                } else {
                                    dep.dependencies = self
                                        .clients
                                        .get(&dep.type_)
                                        .unwrap()
                                        .dependencies(&dep)
                                        .await;
                                }

                                self.dag.add_node_to_dag(dep);
                            }
                        }
                    }
                }
                Event::Shutdown(_et) => {
                    done = done + 1;

                    if done == self.clients.keys().len() {
                        break;
                    }
                }
            }
        }

        if !is_dag(&self.dag.graph) {
            return Err(Error::Dag(
                "Failed constructing dependency graph of the resources".to_string(),
            ));
        }

        if let Some(dot) = self.dag.get_dot() {
            trace!("{}", dot);
        }

        Ok(())
    }

    /// Prints resources to console
    pub async fn print_resources(&mut self) -> Result<()> {
        self.build_dag().await?;

        for r in self.dag.order_by_dependencies()? {
            println!("{}", r);
        }

        Ok(())
    }

    /// Cleans up resources for a particular region across all targeted clients
    pub async fn cleanup_resources(&mut self) -> Result<()> {
        for resource in self.dag.order_by_dependencies()? {
            self.clients
                .entry(resource.type_)
                // For handling clients required for dependent resources
                .or_insert(create_client(
                    &resource.type_,
                    &self.client_details,
                    &self.config,
                    self.dry_run,
                ))
                .cleanup(&resource)
                .await?;
        }
        trace!("Done cleaning up resources");

        Ok(())
    }
}

fn credentials_provider(profile: &Option<String>) -> Result<ChainProvider> {
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

fn create_client(rt: &Client, cd: &ClientDetails, c: &Config, dr: bool) -> Box<dyn NukerClient> {
    match rt {
        Client::Asg => {
            Box::new(AsgClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::EbsSnapshot => {
            Box::new(EbsSnapshotClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::EbsVolume => {
            Box::new(EbsVolumeClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::Ec2Address => {
            Box::new(Ec2AddressClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::Ec2Eni => {
            Box::new(Ec2EniClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::Ec2Igw => {
            Box::new(Ec2IgwClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::Ec2Instance => {
            Box::new(Ec2InstanceClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::Ec2NatGW => {
            Box::new(Ec2NatGWClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::Ec2NetworkACL => {
            Box::new(Ec2NetworkAclClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::Ec2PeeringConnection => {
            Box::new(Ec2PeerConnClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::Ec2RouteTable => {
            Box::new(Ec2RtClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::Ec2Sg => {
            Box::new(Ec2SgClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::Ec2Subnet => {
            Box::new(Ec2SubnetClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::Ec2Vpc => {
            Box::new(Ec2VpcClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::Ec2VpcEndpoint => {
            Box::new(Ec2VpcEndpointClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::Ec2VpnGW => {
            Box::new(Ec2VpnGWClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::EcsCluster => {
            Box::new(EcsClusterClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::EksCluster => {
            Box::new(EksClusterClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::ElbAlb => {
            Box::new(ElbAlbClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::ElbNlb => {
            Box::new(ElbNlbClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::EmrCluster => {
            Box::new(EmrClusterClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::EsDomain => {
            Box::new(EsDomainClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::GlueEndpoint => {
            Box::new(GlueEndpointClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::RdsCluster => {
            Box::new(RdsClusterClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::RdsInstance => {
            Box::new(RdsInstanceClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::RsCluster => {
            Box::new(RsClusterClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::S3Bucket => {
            Box::new(S3BucketClient::new(cd, c.get(&rt).unwrap(), dr)) as Box<dyn NukerClient>
        }
        Client::SagemakerNotebook => {
            Box::new(SagemakerNotebookClient::new(cd, c.get(&rt).unwrap(), dr))
                as Box<dyn NukerClient>
        }
        Client::DefaultClient => {
            panic!("Default client cannot be initiated");
        }
    }
}

fn create_cw_client(
    profile: &Option<String>,
    region: &Region,
    config: &mut Config,
) -> Result<Arc<Box<CwClient>>> {
    let cw_client: rusoto_cloudwatch::CloudWatchClient =
        rusoto_cloudwatch::CloudWatchClient::new_with_client(
            RClient::new_with(credentials_provider(profile)?, HttpClient::new()?),
            region.to_owned(),
        );

    Ok(Arc::new(Box::new(CwClient {
        client: cw_client,
        ec2_idle_rules: config
            .get_mut(&Client::Ec2Instance)
            .unwrap()
            .idle_rules
            .take(),
        ebs_idle_rules: config
            .get_mut(&Client::EbsVolume)
            .unwrap()
            .idle_rules
            .take(),
        elb_alb_idle_rules: config.get_mut(&Client::ElbAlb).unwrap().idle_rules.take(),
        elb_nlb_idle_rules: config.get_mut(&Client::ElbNlb).unwrap().idle_rules.take(),
        rds_idle_rules: config
            .get_mut(&Client::RdsInstance)
            .unwrap()
            .idle_rules
            .take(),
        aurora_idle_rules: config
            .get_mut(&Client::RdsCluster)
            .unwrap()
            .idle_rules
            .take(),
        redshift_idle_rules: config
            .get_mut(&Client::RsCluster)
            .unwrap()
            .idle_rules
            .take(),
        emr_idle_rules: config
            .get_mut(&Client::EmrCluster)
            .unwrap()
            .idle_rules
            .take(),
        es_idle_rules: config.get_mut(&Client::EsDomain).unwrap().idle_rules.take(),
        ecs_idle_rules: config
            .get_mut(&Client::EcsCluster)
            .unwrap()
            .idle_rules
            .take(),
        eks_idle_rules: config
            .get_mut(&Client::EksCluster)
            .unwrap()
            .idle_rules
            .take(),
    })))
}
