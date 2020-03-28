use crate::{
    aws::{cloudwatch::CwClient, util},
    config::{EsConfig, RequiredTags},
    handle_future, handle_future_with_return,
    resource::{EnforcementState, NTag, Resource, ResourceType},
    service::NukerService,
    Result,
};
use async_trait::async_trait;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use rusoto_es::{
    DeleteElasticsearchDomainRequest, DescribeElasticsearchDomainRequest, DomainInfo,
    ElasticsearchDomainStatus, Es, EsClient, ListTagsRequest, Tag,
};
use std::sync::Arc;
use tracing::{debug, trace};

#[derive(Clone)]
pub struct EsService {
    pub client: EsClient,
    pub cw_client: Arc<Box<CwClient>>,
    pub config: EsConfig,
    pub region: Region,
    pub dry_run: bool,
}

impl EsService {
    pub fn new(
        profile_name: Option<String>,
        region: Region,
        config: EsConfig,
        cw_client: Arc<Box<CwClient>>,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = &profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(EsService {
                client: EsClient::new_with(HttpClient::new()?, pp.clone(), region.clone()),
                cw_client,
                config,
                region,
                dry_run,
            })
        } else {
            Ok(EsService {
                client: EsClient::new(region.clone()),
                cw_client,
                config,
                region,
                dry_run,
            })
        }
    }

    async fn package_clusters_as_resources(
        &self,
        domains: Vec<ElasticsearchDomainStatus>,
    ) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for domain in domains {
            let domain_name = domain.domain_name.to_owned();
            let domain_arn = domain.arn.to_owned();
            let mut domain_state: Option<String> = None;

            if let Some(created) = domain.created {
                if created {
                    domain_state = Some("Created".to_string());
                } else {
                    domain_state = Some("Creating".to_string());
                }
            }

            if let Some(true) = domain.deleted {
                domain_state = Some("Deleting".to_string());
            }

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&domain_name) {
                    EnforcementState::SkipConfig
                } else if domain_state != Some("Created".to_string()) {
                    EnforcementState::SkipStopped
                } else {
                    if self.resource_tags_does_not_match(&domain).await {
                        debug!(
                            resource = domain_name.as_str(),
                            "Domain tags does not match"
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.resource_types_does_not_match(&domain).await {
                        debug!(
                            resource = domain_name.as_str(),
                            "Domain instance types does not match"
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.is_resource_idle(&domain).await {
                        debug!(resource = domain_name.as_str(), "Domain is idle");
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else {
                        EnforcementState::Skip
                    }
                }
            };

            resources.push(Resource {
                id: domain_name,
                arn: Some(domain_arn.clone()),
                region: self.region.clone(),
                resource_type: ResourceType::EsDomain,
                tags: self.package_tags_as_ntags(self.list_tags(domain_arn.clone()).await),
                state: domain_state,
                enforcement_state,
                dependencies: None,
            })
        }

        Ok(resources)
    }

    async fn resource_tags_does_not_match(&self, domain: &ElasticsearchDomainStatus) -> bool {
        if self.config.required_tags.is_some() {
            !self.check_tags(
                &self.list_tags(domain.arn.clone()).await,
                &self.config.required_tags.as_ref().unwrap(),
            )
        } else {
            false
        }
    }

    async fn resource_types_does_not_match(&self, domain: &ElasticsearchDomainStatus) -> bool {
        if !self.config.allowed_instance_types.is_empty() {
            !self.config.allowed_instance_types.contains(
                &domain
                    .elasticsearch_cluster_config
                    .instance_type
                    .as_ref()
                    .unwrap(),
            )
        } else {
            false
        }
    }

    async fn is_resource_idle(&self, domain: &ElasticsearchDomainStatus) -> bool {
        if self.config.idle_rules.is_some() {
            !self.cw_client.filter_es_domain(&domain.domain_name).await
        } else {
            false
        }
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

    async fn list_tags(&self, arn: String) -> Option<Vec<Tag>> {
        let req = self.client.list_tags(ListTagsRequest { arn });

        handle_future_with_return!(req)
            .ok()
            .unwrap_or_default()
            .tag_list
    }

    fn check_tags(&self, tags: &Option<Vec<Tag>>, required_tags: &Vec<RequiredTags>) -> bool {
        let ntags = self.package_tags_as_ntags(tags.to_owned());
        util::compare_tags(ntags, required_tags)
    }

    fn package_tags_as_ntags(&self, tags: Option<Vec<Tag>>) -> Option<Vec<NTag>> {
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
impl NukerService for EsService {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized Elastic resource scanner");
        let clusters = self.get_clusters().await?;

        Ok(self.package_clusters_as_resources(clusters).await?)
    }

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.terminate_resource(resource.id.to_owned()).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.terminate_resource(resource.id.to_owned()).await
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
