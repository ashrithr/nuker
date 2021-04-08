use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_sagemaker::{
    DeleteNotebookInstanceInput, ListNotebookInstancesInput, ListTagsInput,
    NotebookInstanceSummary, SageMaker, SageMakerClient, StopNotebookInstanceInput, Tag,
};
use std::str::FromStr;
use tracing::{debug, trace};

#[derive(Clone)]
pub struct SagemakerNotebookClient {
    client: SageMakerClient,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl SagemakerNotebookClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        SagemakerNotebookClient {
            client: SageMakerClient::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn get_notebooks(&self) -> Result<Vec<NotebookInstanceSummary>> {
        let mut next_token: Option<String> = None;
        let mut notebooks: Vec<NotebookInstanceSummary> = Vec::new();

        loop {
            let req = self
                .client
                .list_notebook_instances(ListNotebookInstancesInput {
                    next_token,
                    ..Default::default()
                });

            if let Ok(result) = handle_future_with_return!(req) {
                if let Some(ns) = result.notebook_instances {
                    for n in ns {
                        notebooks.push(n);
                    }
                }

                if result.next_token.is_none() {
                    break;
                } else {
                    next_token = result.next_token;
                }
            } else {
                break;
            }
        }

        Ok(notebooks)
    }

    async fn package_resources(
        &self,
        notebooks: Vec<NotebookInstanceSummary>,
    ) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for notebook in notebooks {
            let notebook_id = notebook.notebook_instance_name;
            let arn = notebook.notebook_instance_arn;
            let tags = self.get_tags(&arn).await;
            let ntags = self.package_tags(tags);

            resources.push(Resource {
                id: notebook_id,
                arn: Some(arn),
                type_: ClientType::SagemakerNotebook,
                region: self.region.clone(),
                tags: ntags,
                state: ResourceState::from_str(
                    notebook
                        .notebook_instance_status
                        .as_deref()
                        .unwrap_or_default(),
                )
                .ok(),
                start_time: Some(format!("{}", notebook.creation_time.unwrap_or(0f64) as i64)),
                resource_type: notebook.instance_type.map(|t| vec![t]),
                enforcement_state: EnforcementState::SkipUnknownState,
                enforcement_reason: None,
                dependencies: None,
                termination_protection: None,
            })
        }

        Ok(resources)
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

    async fn get_tags(&self, arn: &str) -> Option<Vec<Tag>> {
        let req = self.client.list_tags(ListTagsInput {
            resource_arn: arn.to_string(),
            ..Default::default()
        });

        handle_future_with_return!(req)
            .ok()
            .map(|r| r.tags)
            .unwrap_or_default()
    }

    async fn delete_notebook(&self, notebook_id: &str) -> Result<()> {
        debug!(resource = notebook_id, "Deleting");

        if !self.dry_run {
            let req = self
                .client
                .delete_notebook_instance(DeleteNotebookInstanceInput {
                    notebook_instance_name: notebook_id.to_owned(),
                });

            handle_future!(req);
        }

        Ok(())
    }

    async fn stop_notebook(&self, notebook_id: &str) -> Result<()> {
        debug!(resource = notebook_id, "Stopping");

        if !self.dry_run {
            let req = self
                .client
                .stop_notebook_instance(StopNotebookInstanceInput {
                    notebook_instance_name: notebook_id.to_owned(),
                });
            handle_future!(req);
        }

        Ok(())
    }
}

#[async_trait]
impl NukerClient for SagemakerNotebookClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized Sagemaker resource scanner");

        let notebooks = self.get_notebooks().await?;

        Ok(self.package_resources(notebooks).await?)
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

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.stop_notebook(resource.id.as_ref()).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_notebook(resource.id.as_ref()).await
    }
}
