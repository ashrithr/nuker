use crate::{
    aws::{util, Result},
    config::{RequiredTags, SagemakerConfig},
    service::{EnforcementState, NTag, NukeService, Resource, ResourceType},
};
use chrono::{TimeZone, Utc};
use log::debug;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use rusoto_sagemaker::{
    DeleteNotebookInstanceInput, ListNotebookInstancesInput, ListTagsInput,
    NotebookInstanceSummary, SageMaker, SageMakerClient, StopNotebookInstanceInput, Tag,
};

pub struct SagemakerNukeClient {
    pub client: SageMakerClient,
    pub config: SagemakerConfig,
    pub region: Region,
    pub dry_run: bool,
}

impl SagemakerNukeClient {
    pub fn new(
        profile_name: Option<&str>,
        region: Region,
        config: SagemakerConfig,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(SagemakerNukeClient {
                client: SageMakerClient::new_with(HttpClient::new()?, pp, region.clone()),
                config,
                region,
                dry_run,
            })
        } else {
            Ok(SagemakerNukeClient {
                client: SageMakerClient::new(region.clone()),
                config,
                region,
                dry_run,
            })
        }
    }

    fn get_notebooks(&self) -> Result<Vec<NotebookInstanceSummary>> {
        let mut next_token: Option<String> = None;
        let mut notebooks: Vec<NotebookInstanceSummary> = Vec::new();

        loop {
            let result = self
                .client
                .list_notebook_instances(ListNotebookInstancesInput {
                    next_token,
                    ..Default::default()
                })
                .sync()?;

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
        }

        Ok(notebooks)
    }

    fn package_notebooks_as_resources(
        &self,
        notebooks: Vec<NotebookInstanceSummary>,
    ) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for notebook in notebooks {
            let notebook_id = notebook.notebook_instance_name.clone();
            let tags = self.get_tags(&notebook)?;
            let ntags = self.package_tags_as_ntags(tags);

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&notebook_id) {
                    debug!("Skipping resource from ignore list - {}", notebook_id);
                    EnforcementState::SkipConfig
                } else if notebook.notebook_instance_status != Some("InService".to_string()) {
                    debug!("Skipping resource is not running - {}", notebook_id);
                    EnforcementState::SkipStopped
                } else {
                    if self.resource_tags_does_not_match(ntags.clone()) {
                        debug!("Resource tags does not match - {}", notebook_id);
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.resource_types_does_not_match(&notebook) {
                        debug!("Resource types does not match - {}", notebook_id);
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.resource_allowed_run_time(&notebook) {
                        debug!(
                            "Resource is running beyond allowed run time ({:?}) - {}",
                            self.config.older_than, notebook_id
                        );
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else {
                        EnforcementState::Skip
                    }
                }
            };

            resources.push(Resource {
                id: notebook_id,
                resource_type: ResourceType::SagemakerNotebook,
                region: self.region.clone(),
                tags: ntags,
                state: notebook.notebook_instance_status,
                enforcement_state,
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
            let notebook_start = Utc.timestamp(notebook.creation_time.unwrap() as i64, 0);
            let start = Utc::now().timestamp_millis() - self.config.older_than.as_millis() as i64;

            if start > notebook_start.timestamp_millis() {
                true
            } else {
                false
            }
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

    fn get_tags(&self, notebook: &NotebookInstanceSummary) -> Result<Option<Vec<Tag>>> {
        let result = self
            .client
            .list_tags(ListTagsInput {
                resource_arn: notebook.notebook_instance_arn.clone(),
                ..Default::default()
            })
            .sync()?;

        Ok(result.tags)
    }

    fn check_tags(&self, ntags: Option<Vec<NTag>>, required_tags: &Vec<RequiredTags>) -> bool {
        util::compare_tags(ntags, required_tags)
    }

    fn delete_notebook(&self, notebook_id: &str) -> Result<()> {
        debug!("Deleting the Sagemaker notebook instance: {}", notebook_id);

        if !self.dry_run {
            self.client
                .delete_notebook_instance(DeleteNotebookInstanceInput {
                    notebook_instance_name: notebook_id.to_owned(),
                })
                .sync()?;
        }

        Ok(())
    }

    fn stop_notebook(&self, notebook_id: &str) -> Result<()> {
        debug!("Stopping the Sagemaker notebook instance: {}", notebook_id);

        if !self.dry_run {
            self.client
                .stop_notebook_instance(StopNotebookInstanceInput {
                    notebook_instance_name: notebook_id.to_owned(),
                })
                .sync()?;
        }

        Ok(())
    }
}

impl NukeService for SagemakerNukeClient {
    fn scan(&self) -> Result<Vec<Resource>> {
        let notebooks = self.get_notebooks()?;

        Ok(self.package_notebooks_as_resources(notebooks)?)
    }

    fn stop(&self, resource: &Resource) -> Result<()> {
        self.stop_notebook(resource.id.as_ref())
    }

    fn delete(&self, resource: &Resource) -> Result<()> {
        if resource.state == Some("Stopped".to_string())
            || resource.state == Some("Failed".to_string())
        {
            self.delete_notebook(resource.id.as_ref())
        } else {
            self.stop(resource)
        }
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
