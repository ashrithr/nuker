use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_ec2::{
    DeleteSecurityGroupRequest, DescribeSecurityGroupsRequest, Ec2, Ec2Client, SecurityGroup, Tag,
};
use std::collections::{HashMap, HashSet};
use tracing::{debug, trace};

#[derive(Clone)]
pub struct Ec2SgClient {
    client: Ec2Client,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl Ec2SgClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        Ec2SgClient {
            client: Ec2Client::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn package_resources(&self, mut sgs: Vec<SecurityGroup>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for sg in &mut sgs {
            let arn = format!(
                "arn:aws:ec2:{}:{}:security-group/{}",
                self.region.name(),
                self.account_num,
                sg.group_id.as_ref().unwrap(),
            );

            resources.push(Resource {
                id: sg.group_id.take().unwrap(),
                arn: Some(arn),
                type_: ClientType::Ec2Sg,
                region: self.region.clone(),
                tags: self.package_tags(sg.tags.take()),
                state: None,
                start_time: None,
                enforcement_state: EnforcementState::SkipUnknownState,
                resource_type: None,
                dependencies: None,
                termination_protection: None,
            });
        }

        Ok(resources)
    }

    async fn get_sgs(&self) -> Result<Vec<SecurityGroup>> {
        let mut next_token: Option<String> = None;
        let mut sgs: Vec<SecurityGroup> = Vec::new();

        loop {
            let req = self
                .client
                .describe_security_groups(DescribeSecurityGroupsRequest {
                    next_token,
                    ..Default::default()
                });

            if let Ok(result) = handle_future_with_return!(req) {
                if let Some(security_groups) = result.security_groups {
                    for sg in security_groups {
                        sgs.push(sg);
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

        Ok(sgs)
    }

    // TODO: handle self referencing rule - remove it
    async fn delete_sg(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting.");

        if !self.dry_run {
            let req = self
                .client
                .delete_security_group(DeleteSecurityGroupRequest {
                    group_id: Some(resource.id.clone()),
                    ..Default::default()
                });

            handle_future!(req);
        }

        Ok(())
    }

    fn package_tags(&self, tags: Option<Vec<Tag>>) -> Option<Vec<NTag>> {
        tags.map(|ts| {
            ts.into_iter()
                .map(|mut tag| NTag {
                    key: std::mem::replace(&mut tag.key, None),
                    value: std::mem::replace(&mut tag.value, None),
                })
                .collect()
        })
    }

    async fn get_dependencies(&self, resource: &Resource) -> Result<Vec<Resource>> {
        let mut dependencies: Vec<Resource> = Vec::new();
        let mut deps_map: HashMap<String, HashSet<String>> = HashMap::new();
        let rid = resource.id.clone();

        // Find all the dependent security groups
        if let Some(mut sgs) = self.get_sgs().await.ok() {
            for sg in &mut sgs {
                let group_name = sg.group_name.take().unwrap();
                let group_id = sg.group_id.take().unwrap();

                if !deps_map.contains_key(&group_id) {
                    deps_map.insert(group_id.clone(), HashSet::new());
                }

                for rule in sg.ip_permissions.take().unwrap() {
                    if let Some(grants) = rule.user_id_group_pairs {
                        for grant in grants {
                            if let Some(gid) = grant.group_id {
                                if !deps_map.contains_key(&gid) {
                                    deps_map.insert(gid.clone(), HashSet::default());
                                }
                                deps_map.get_mut(&gid).unwrap().insert(group_id.clone());
                            }
                        }
                    }
                }
            }
        }

        unimplemented!()
    }
}

#[async_trait]
impl NukerClient for Ec2SgClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized EC2 SG resource scanner");
        let sgs = self.get_sgs().await?;
        Ok(self.package_resources(sgs).await?)
    }

    async fn dependencies(&self, resource: &Resource) -> Option<Vec<Resource>> {
        self.get_dependencies(resource).await.ok()
    }

    fn additional_filters(&self, _resource: &Resource, _config: &ResourceConfig) -> Option<bool> {
        None
    }

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.delete_sg(resource).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_sg(resource).await
    }
}
