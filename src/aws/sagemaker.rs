use crate::{
    aws::util,
    config::{RequiredTags, SagemakerConfig},
    handle_future, handle_future_with_return,
    resource::{EnforcementState, NTag, Resource, ResourceType},
    service::NukerService,
    Result,
};
use async_trait::async_trait;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use rusoto_sagemaker::{
    DeleteNotebookInstanceInput, ListNotebookInstancesInput, ListTagsInput,
    NotebookInstanceSummary, SageMaker, SageMakerClient, StopNotebookInstanceInput, Tag,
};
use tracing::{debug, trace};

#[derive(Clone)]
pub struct SagemakerService {
    pub client: SageMakerClient,
    pub config: SagemakerConfig,
    pub region: Region,
    pub dry_run: bool,
}

impl SagemakerService {
    pub fn new(
        profile_name: Option<String>,
        region: Region,
        config: SagemakerConfig,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = &profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(SagemakerService {
                client: SageMakerClient::new_with(HttpClient::new()?, pp, region.clone()),
                config,
                region,
                dry_run,
            })
        } else {
            Ok(SagemakerService {
                client: SageMakerClient::new(region.clone()),
                config,
                region,
                dry_run,
            })
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

    async fn package_notebooks_as_resources(
        &self,
        notebooks: Vec<NotebookInstanceSummary>,
    ) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for notebook in notebooks {
            let notebook_id = notebook.notebook_instance_name.clone();
            let tags = self.get_tags(&notebook).await;
            let ntags = self.package_tags_as_ntags(tags);

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&notebook_id) {
                    debug!(
                        resource = notebook_id.as_str(),
                        "Skipping notebook from ignore list"
                    );
                    EnforcementState::SkipConfig
                } else if notebook.notebook_instance_status != Some("InService".to_string()) {
                    debug!(
                        resource = notebook_id.as_str(),
                        "Skipping notebook is not running"
                    );
                    EnforcementState::SkipStopped
                } else {
                    if self.resource_tags_does_not_match(ntags.clone()) {
                        debug!(
                            resource = notebook_id.as_str(),
                            "Notebook tags does not match"
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.resource_types_does_not_match(&notebook) {
                        debug!(
                            resource = notebook_id.as_str(),
                            "Notebook types does not match"
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.resource_allowed_run_time(&notebook) {
                        debug!(
                            resource = notebook_id.as_str(),
                            "Notebook is running beyond allowed run time ({:?})",
                            self.config.older_than,
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else {
                        EnforcementState::Skip
                    }
                }
            };

            resources.push(Resource {
                id: notebook_id,
                arn: Some(notebook.notebook_instance_arn),
                resource_type: ResourceType::SagemakerNotebook,
                region: self.region.clone(),
                tags: ntags,
                state: notebook.notebook_instance_status,
                enforcement_state,
                dependencies: None,
            })
        }

        Ok(resources)
    }

    fn resource_types_does_not_match(&self, notebook: &NotebookInstanceSummary) -> bool {
        if !self.config.allowed_instance_types.is_empty() {
            !self
                .config
                .allowed_instance_types
                .contains(&notebook.instance_type.as_ref().unwrap().clone())
        } else {
            false
        }
    }

    fn resource_allowed_run_time(&self, notebook: &NotebookInstanceSummary) -> bool {
        if self.config.older_than.as_secs() > 0 && notebook.creation_time.is_some() {
            let date = format!("{}", notebook.creation_time.unwrap_or(0f64) as i64);
            util::is_ts_older_than(date.as_str(), &self.config.older_than)
        } else {
            false
        }
    }

    fn resource_tags_does_not_match(&self, ntags: Option<Vec<NTag>>) -> bool {
        if self.config.required_tags.is_some() {
            !self.check_tags(ntags, &self.config.required_tags.as_ref().unwrap())
        } else {
            false
        }
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

    async fn get_tags(&self, notebook: &NotebookInstanceSummary) -> Option<Vec<Tag>> {
        let req = self.client.list_tags(ListTagsInput {
            resource_arn: notebook.notebook_instance_arn.clone(),
            ..Default::default()
        });

        handle_future_with_return!(req)
            .ok()
            .map(|r| r.tags)
            .unwrap_or_default()
    }

    fn check_tags(&self, ntags: Option<Vec<NTag>>, required_tags: &Vec<RequiredTags>) -> bool {
        util::compare_tags(ntags, required_tags)
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
impl NukerService for SagemakerService {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized Sagemaker resource scanner");

        let notebooks = self.get_notebooks().await?;

        Ok(self.package_notebooks_as_resources(notebooks).await?)
    }

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.stop_notebook(resource.id.as_ref()).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        if resource.state == Some("Stopped".to_string())
            || resource.state == Some("Failed".to_string())
        {
            self.delete_notebook(resource.id.as_ref()).await
        } else {
            self.stop(resource).await
        }
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
