use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_es::{
    DeleteElasticsearchDomainRequest, DescribeElasticsearchDomainConfigRequest,
    DescribeElasticsearchDomainRequest, DomainInfo, ElasticsearchDomainConfig,
    ElasticsearchDomainStatus, Es, EsClient, ListTagsRequest, Tag,
};
use std::str::FromStr;
use tracing::{debug, trace};

#[derive(Clone)]
pub struct EsDomainClient {
    client: EsClient,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl EsDomainClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        EsDomainClient {
            client: EsClient::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn package_resources(
        &self,
        domains: Vec<ElasticsearchDomainStatus>,
    ) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for domain in domains {
            let mut domain_state: Option<String> = None;

            if let Some(created) = domain.created {
                if created {
                    domain_state = Some("running".to_string());
                } else {
                    domain_state = Some("starting".to_string());
                }
            }

            if let Some(true) = domain.deleted {
                domain_state = Some("deleting".to_string());
            }

            let tags = self.list_tags(domain.arn.as_str()).await;
            let instance_type = domain
                .elasticsearch_cluster_config
                .instance_type
                .map(|it| vec![it]);
            let start_time = if self.config.max_run_time.is_some() {
                self.get_domain_config(domain.domain_name.as_str())
                    .await
                    .and_then(|dc| dc.elasticsearch_cluster_config)
                    .and_then(|c| Some(c.status))
                    .and_then(|s| Some(format!("{}", s.creation_date as i64)))
            } else {
                None
            };

            resources.push(Resource {
                id: domain.domain_name,
                arn: Some(domain.arn),
                type_: ClientType::EsDomain,
                region: self.region.clone(),
                tags: self.package_tags(tags),
                state: ResourceState::from_str(domain_state.unwrap_or_default().as_str()).ok(),
                start_time,
                enforcement_state: EnforcementState::SkipUnknownState,
                enforcement_reason: None,
                resource_type: instance_type,
                dependencies: None,
                termination_protection: None,
            });
        }

        Ok(resources)
    }

    async fn get_clusters(&self) -> Result<Vec<ElasticsearchDomainStatus>> {
        let mut clusters: Vec<ElasticsearchDomainStatus> = Vec::new();

        if let Some(domains) = self.get_domains().await {
            for domain in domains {
                if let Some(domain_name) = domain.domain_name {
                    let req = self.client.describe_elasticsearch_domain(
                        DescribeElasticsearchDomainRequest { domain_name },
                    );
                    if let Ok(result) = handle_future_with_return!(req) {
                        clusters.push(result.domain_status)
                    }
                }
            }
        }

        Ok(clusters)
    }

    async fn get_domains(&self) -> Option<Vec<DomainInfo>> {
        let req = self.client.list_domain_names();

        handle_future_with_return!(req)
            .ok()
            .unwrap_or_default()
            .domain_names
    }

    async fn list_tags(&self, arn: &str) -> Option<Vec<Tag>> {
        let req = self.client.list_tags(ListTagsRequest {
            arn: arn.to_string(),
        });

        handle_future_with_return!(req)
            .ok()
            .unwrap_or_default()
            .tag_list
    }

    async fn get_domain_config(&self, domain_name: &str) -> Option<ElasticsearchDomainConfig> {
        let req = self.client.describe_elasticsearch_domain_config(
            DescribeElasticsearchDomainConfigRequest {
                domain_name: domain_name.to_string(),
            },
        );

        handle_future_with_return!(req)
            .ok()
            .map(|resp| resp.domain_config)
    }

    fn package_tags(&self, tags: Option<Vec<Tag>>) -> Option<Vec<NTag>> {
        tags.map(|ts| {
            ts.iter()
                .map(|tag| NTag {
                    key: Some(tag.key.clone()),
                    value: Some(tag.value.clone()),
                })
                .collect()
        })
    }

    async fn terminate_resource(&self, domain_name: String) -> Result<()> {
        debug!(resource = domain_name.as_str(), "Deleting");

        if !self.dry_run {
            let req = self
                .client
                .delete_elasticsearch_domain(DeleteElasticsearchDomainRequest { domain_name });
            handle_future!(req);
        }

        Ok(())
    }
}

#[async_trait]
impl NukerClient for EsDomainClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized Elastic resource scanner");
        let clusters = self.get_clusters().await?;

        Ok(self.package_resources(clusters).await?)
    }

    async fn dependencies(&self, _resource: &Resource) -> Option<Vec<Resource>> {
        None
    }

    async fn additional_filters(
        &self,
        _resource: &Resource,
        _config: &ResourceConfig,
    ) -> Option<bool> {
        None
    }

    async fn stop(&self, _resource: &Resource) -> Result<()> {
        Ok(())
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.terminate_resource(resource.id.to_owned()).await
    }
}
