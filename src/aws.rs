mod cloudwatch;
mod ec2_instance;
mod rds_cluster;
mod rds_instance;
mod sts;

pub use cloudwatch::CwClient;

use crate::Event;
use crate::{
    aws::{
        ec2_instance::Ec2Instance, rds_cluster::RdsClusterClient, rds_instance::RdsInstanceClient,
        sts::StsService,
    },
    client::Client,
    client::NukerClient,
    config::Config,
    graph::{is_dag, Dag},
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
    region: Region,
    client: RClient,
}

/// AWS Nuker for nuking resources in AWS.
pub struct AwsNuker {
    pub region: Region,
    pub config: Config,
    clients: HashMap<Client, Box<dyn NukerClient>>,
    cw_client: Arc<Box<CwClient>>,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    dag: Dag,
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
            match client {
                Client::Ec2Instance => {
                    // TODO: Need a macro for this
                    if !excluded_clients.contains(&client) {
                        clients.insert(
                            Client::Ec2Instance,
                            Box::new(Ec2Instance::new(
                                &client_details,
                                &config.get(&client).unwrap(),
                                dry_run,
                            )),
                        );
                    }
                }
                Client::RdsInstance => {
                    if !excluded_clients.contains(&client) {
                        clients.insert(
                            Client::RdsInstance,
                            Box::new(RdsInstanceClient::new(
                                &client_details,
                                &config.get(&client).unwrap(),
                                dry_run,
                            )),
                        );
                    }
                }
                Client::RdsCluster => {
                    if !excluded_clients.contains(&client) {
                        clients.insert(
                            Client::RdsCluster,
                            Box::new(RdsClusterClient::new(
                                &client_details,
                                &config.get(&client).unwrap(),
                                dry_run,
                            )),
                        );
                    }
                }
                _ => {}
            }
        }

        let (tx, rx) = channel(100);

        Ok(AwsNuker {
            region,
            config,
            clients,
            cw_client,
            tx,
            rx,
            dag: Dag::new(),
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
                Event::Resource(r) => {
                    self.dag.add_node_to_dag(r);
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

        trace!("{:?}", self.dag.get_dot());

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
                .get(&resource.type_)
                .unwrap()
                .cleanup(&resource)
                .await?;
        }
        trace!("Done cleaning up resources");

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
            RClient::new_with(credentials_provider(profile)?, HttpClient::new()?),
            region.to_owned(),
        );

    Ok(Arc::new(Box::new(CwClient {
        client: cw_client,
        ec2_idle_rules: std::mem::replace(
            &mut config.get_mut(&Client::Ec2Instance).unwrap().idle_rules,
            None,
        ),
        ebs_idle_rules: None,
        elb_alb_idle_rules: None,
        elb_nlb_idle_rules: None,
        rds_idle_rules: std::mem::replace(
            &mut config.get_mut(&Client::RdsInstance).unwrap().idle_rules,
            None,
        ),
        aurora_idle_rules: std::mem::replace(
            &mut config.get_mut(&Client::RdsCluster).unwrap().idle_rules,
            None,
        ),
        redshift_idle_rules: None,
        emr_idle_rules: None,
        es_idle_rules: None,
        ecs_idle_rules: None,
    })))
}
