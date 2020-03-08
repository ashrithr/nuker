use crate::{
    aws::{cloudwatch::CwClient, util, Result},
    config::{Ec2Config, RequiredTags},
    resource::{EnforcementState, NTag, Resource, ResourceType},
    service::NukerService,
};
use async_trait::async_trait;
use log::{debug, trace};
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use rusoto_ec2::{
    Address, DeleteNetworkInterfaceRequest, DescribeAddressesRequest,
    DescribeInstanceAttributeRequest, DescribeInstancesRequest, DescribeInstancesResult,
    DescribeNetworkInterfacesRequest, DescribeSecurityGroupsRequest, Ec2, Ec2Client, Filter,
    Instance, ModifyInstanceAttributeRequest, NetworkInterface, ReleaseAddressRequest,
    StopInstancesRequest, Tag, TerminateInstancesRequest,
};

#[derive(Clone)]
pub struct Ec2Service {
    pub client: Ec2Client,
    pub cw_client: CwClient,
    pub config: Ec2Config,
    pub region: Region,
    pub dry_run: bool,
}

impl Ec2Service {
    pub fn new(
        profile_name: Option<String>,
        region: Region,
        config: Ec2Config,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = &profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(Ec2Service {
                client: Ec2Client::new_with(HttpClient::new()?, pp, region.clone()),
                cw_client: CwClient::new(
                    profile_name.clone(),
                    region.clone(),
                    config.clone().idle_rules,
                )?,
                config,
                region,
                dry_run,
            })
        } else {
            Ok(Ec2Service {
                client: Ec2Client::new(region.clone()),
                cw_client: CwClient::new(
                    profile_name.clone(),
                    region.clone(),
                    config.clone().idle_rules,
                )?,
                config,
                region,
                dry_run,
            })
        }
    }

    fn package_tags_as_ntags(&self, tags: Option<Vec<Tag>>) -> Option<Vec<NTag>> {
        tags.map(|ts| {
            ts.iter()
                .map(|tag| NTag {
                    key: tag.key.clone(),
                    value: tag.value.clone(),
                })
                .collect()
        })
    }

    async fn package_instances_as_resources(
        &self,
        instances: Vec<Instance>,
        sgs: Option<Vec<String>>,
    ) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for instance in instances {
            let instance_id = instance.instance_id.as_ref().unwrap().to_owned();

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&instance_id) {
                    // Instance not in ignore list
                    debug!("Skipping resource from ignore list - {}", instance_id);
                    EnforcementState::SkipConfig
                } else if instance.state.as_ref().unwrap().code != Some(16) {
                    // Instance not in running state
                    debug!("Skipping as instance is not running - {}", instance_id);
                    EnforcementState::SkipStopped
                } else {
                    if self.resource_tags_does_not_match(&instance) {
                        debug!("Resource tags does not match - {}", instance_id);
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.resource_types_does_not_match(&instance) {
                        debug!("Resource types does not match - {}", instance_id);
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.is_resource_idle(&instance).await {
                        debug!("Resource is idle - {}", instance_id);
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else if self.is_resource_not_secure(&instance, sgs.clone()) {
                        debug!("Resource is not secure - {}", instance_id);
                        EnforcementState::from_target_state(&self.config.target_state)
                    } else {
                        EnforcementState::Skip
                    }
                }
            };

            resources.push(Resource {
                id: instance_id,
                region: self.region.clone(),
                resource_type: ResourceType::Ec2Instance,
                tags: self.package_tags_as_ntags(instance.tags.clone()),
                state: instance.state.as_ref().unwrap().name.clone(),
                enforcement_state,
            });
        }

        Ok(resources)
    }

    fn package_interfaces_as_resources(
        &self,
        interfaces: Vec<NetworkInterface>,
    ) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for interface in interfaces {
            let interface_id = interface.network_interface_id.as_ref().unwrap().to_owned();

            let enforcement_state: EnforcementState = {
                if interface.attachment.is_some()
                    && interface.attachment.unwrap().status == Some("detached".to_string())
                {
                    debug!("Resource is detached and idle: {}", interface_id);
                    EnforcementState::Delete
                } else {
                    EnforcementState::Skip
                }
            };

            resources.push(Resource {
                id: interface_id,
                region: self.region.clone(),
                resource_type: ResourceType::Ec2Interface,
                tags: self.package_tags_as_ntags(interface.tag_set.clone()),
                state: interface.status,
                enforcement_state,
            });
        }

        Ok(resources)
    }

    fn package_addresses_as_resources(&self, addresses: Vec<Address>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for address in addresses {
            let address_id = address.allocation_id.as_ref().unwrap().to_owned();

            let enforcement_state: EnforcementState = {
                if address.association_id.is_none() {
                    debug!("Resource is detached and idle: {}", address_id);
                    EnforcementState::Delete
                } else {
                    EnforcementState::Skip
                }
            };

            resources.push(Resource {
                id: address_id,
                region: self.region.clone(),
                resource_type: ResourceType::Ec2Address,
                tags: self.package_tags_as_ntags(address.tags.clone()),
                state: None,
                enforcement_state,
            });
        }

        Ok(resources)
    }

    fn resource_tags_does_not_match(&self, instance: &Instance) -> bool {
        if self.config.required_tags.is_some() {
            !self.check_tags(&instance.tags, &self.config.required_tags.as_ref().unwrap())
        } else {
            false
        }
    }

    fn resource_types_does_not_match(&self, instance: &Instance) -> bool {
        if !self.config.allowed_instance_types.is_empty() {
            !self
                .config
                .allowed_instance_types
                .contains(&instance.instance_type.clone().unwrap())
        } else {
            false
        }
    }

    async fn is_resource_idle(&self, instance: &Instance) -> bool {
        if !self.config.idle_rules.is_empty() {
            !self
                .cw_client
                .filter_instance(&instance.instance_id.as_ref().unwrap())
                .await
                .unwrap()
        } else {
            false
        }
    }

    fn is_resource_not_secure(&self, instance: &Instance, sgs: Option<Vec<String>>) -> bool {
        if self.config.security_groups.enabled && sgs.is_some() {
            let instance_sgs = instance
                .security_groups
                .clone()
                .unwrap()
                .iter()
                .map(|gi| gi.group_id.clone().unwrap())
                .collect::<Vec<String>>();

            sgs.unwrap().iter().any(|s| instance_sgs.contains(&s))
        } else {
            false
        }
    }

    async fn get_instances(&self, filter: Vec<Filter>) -> Result<Vec<Instance>> {
        let mut next_token: Option<String> = None;
        let mut instances: Vec<Instance> = Vec::new();

        loop {
            let result: DescribeInstancesResult = self
                .client
                .describe_instances(DescribeInstancesRequest {
                    dry_run: None,
                    filters: Some(filter.clone()),
                    instance_ids: None,
                    max_results: None,
                    next_token,
                })
                .await?;

            if let Some(reservations) = result.reservations {
                let reservations: Vec<Vec<Instance>> = reservations
                    .into_iter()
                    .filter_map(|reservation| reservation.instances)
                    .collect();

                let mut temp_instances: Vec<Instance> =
                    reservations.into_iter().flatten().collect();

                instances.append(&mut temp_instances);
            }

            if result.next_token.is_none() {
                break;
            } else {
                next_token = result.next_token;
            }
        }

        Ok(instances)
    }

    /// Checks instance tags against required tags and returns true only if all required tags are
    /// present
    fn check_tags(&self, tags: &Option<Vec<Tag>>, required_tags: &Vec<RequiredTags>) -> bool {
        let ntags = self.package_tags_as_ntags(tags.to_owned());
        util::compare_tags(ntags, required_tags)
    }

    async fn disable_termination_protection(&self, instance_id: &str) -> Result<()> {
        let resp = self
            .client
            .describe_instance_attribute(DescribeInstanceAttributeRequest {
                attribute: "disableApiTermination".into(),
                instance_id: instance_id.into(),
                ..Default::default()
            })
            .await?;

        if resp.disable_api_termination.unwrap().value.unwrap() {
            debug!(
                "Terminating protection was enabled for: {}. Trying to Disable it.",
                instance_id
            );

            if !self.dry_run {
                self.client
                    .modify_instance_attribute(ModifyInstanceAttributeRequest {
                        attribute: Some("disableApiTermination".into()),
                        instance_id: instance_id.into(),
                        ..Default::default()
                    })
                    .await?;
            }
        }

        Ok(())
    }

    async fn delete_resource(&self, resource: &Resource) -> Result<()> {
        debug!("Deleting the resource: {:?}", resource.id);

        match resource.resource_type {
            ResourceType::Ec2Instance => {
                if self.config.termination_protection.ignore {
                    self.disable_termination_protection(resource.id.as_ref())
                        .await?;
                }

                if !self.dry_run {
                    self.client
                        .terminate_instances(TerminateInstancesRequest {
                            instance_ids: vec![resource.id.clone()],
                            ..Default::default()
                        })
                        .await?;
                }
            }
            ResourceType::Ec2Interface => {
                self.delete_interface(resource).await?;
            }
            ResourceType::Ec2Address => {
                self.delete_address(resource).await?;
            }
            _ => {}
        }

        Ok(())
    }

    async fn stop_resource(&self, resource: &Resource) -> Result<()> {
        match resource.resource_type {
            ResourceType::Ec2Instance => {
                debug!("Stopping Resource: {:?}", resource.id);

                if !self.dry_run {
                    self.client
                        .stop_instances(StopInstancesRequest {
                            instance_ids: vec![resource.id.clone()],
                            force: Some(true),
                            ..Default::default()
                        })
                        .await?;
                }
            }
            ResourceType::Ec2Interface | ResourceType::Ec2Address => {
                self.delete_resource(resource).await?;
            }
            _ => {}
        }

        Ok(())
    }

    async fn get_open_sgs(&self) -> Result<Vec<String>> {
        self.get_security_groups(Some(vec![
            Filter {
                name: Some("ip-permission.cidr".to_string()),
                values: Some(self.config.security_groups.source_cidr.clone()),
            },
            Filter {
                name: Some("ip-permission.from-port".to_string()),
                values: Some(vec![self.config.security_groups.from_port.to_string()]),
            },
            Filter {
                name: Some("ip-permission.to-port".to_string()),
                values: Some(vec![self.config.security_groups.to_port.to_string()]),
            },
        ]))
        .await
    }

    async fn get_default_sgs(&self) -> Result<Vec<String>> {
        self.get_security_groups(Some(vec![Filter {
            name: Some("group-name".to_string()),
            values: Some(vec!["default".to_string(), "launch-wizard-*".to_string()]),
        }]))
        .await
    }

    async fn get_security_groups(&self, filters: Option<Vec<Filter>>) -> Result<Vec<String>> {
        let mut next_token: Option<String> = None;
        let mut security_groups: Vec<String> = Vec::new();

        loop {
            let result = self
                .client
                .describe_security_groups(DescribeSecurityGroupsRequest {
                    filters: filters.clone(),
                    next_token,
                    ..Default::default()
                })
                .await?;

            if let Some(sgs) = result.security_groups {
                for sg in sgs {
                    security_groups.push(sg.group_id.unwrap_or_default())
                }
            }

            if result.next_token.is_none() {
                break;
            } else {
                next_token = result.next_token;
            }
        }

        Ok(security_groups)
    }

    async fn get_network_interfaces(&self) -> Result<Vec<NetworkInterface>> {
        let mut next_token: Option<String> = None;
        let mut interfaces: Vec<NetworkInterface> = Vec::new();

        loop {
            let result = self
                .client
                .describe_network_interfaces(DescribeNetworkInterfacesRequest {
                    next_token,
                    ..Default::default()
                })
                .await?;

            if let Some(nics) = result.network_interfaces {
                for nic in nics {
                    interfaces.push(nic);
                }
            }

            if result.next_token.is_none() {
                break;
            } else {
                next_token = result.next_token;
            }
        }

        Ok(interfaces)
    }

    async fn delete_interface(&self, resource: &Resource) -> Result<()> {
        if !self.dry_run {
            self.client
                .delete_network_interface(DeleteNetworkInterfaceRequest {
                    network_interface_id: resource.id.clone(),
                    ..Default::default()
                })
                .await?
        }

        Ok(())
    }

    async fn get_addresses(&self) -> Result<Vec<Address>> {
        let mut addresses: Vec<Address> = Vec::new();

        let result = self
            .client
            .describe_addresses(DescribeAddressesRequest {
                ..Default::default()
            })
            .await?;

        if result.addresses.is_some() {
            addresses.append(&mut result.addresses.unwrap())
        }

        Ok(addresses)
    }

    async fn delete_address(&self, resource: &Resource) -> Result<()> {
        if !self.dry_run {
            self.client
                .release_address(ReleaseAddressRequest {
                    allocation_id: Some(resource.id.to_owned()),
                    ..Default::default()
                })
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl NukerService for Ec2Service {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!(
            "Initialized EC2 resource scanner for {:?} region",
            self.region.name()
        );
        let mut resources: Vec<Resource> = Vec::new();

        let instances = self.get_instances(Vec::new()).await?;
        let sgs: Option<Vec<String>> = if self.config.security_groups.enabled {
            Some(
                self.get_open_sgs()
                    .await?
                    .into_iter()
                    .chain(self.get_default_sgs().await?.into_iter())
                    .collect::<Vec<String>>(),
            )
        } else {
            None
        };
        resources.append(&mut self.package_instances_as_resources(instances, sgs).await?);

        if self.config.eni.cleanup {
            let interfaces = self.get_network_interfaces().await?;
            resources.append(&mut self.package_interfaces_as_resources(interfaces)?);
        }

        if self.config.eip.cleanup {
            let addresses = self.get_addresses().await?;
            resources.append(&mut self.package_addresses_as_resources(addresses)?);
        }

        Ok(resources)
    }

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.stop_resource(resource).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_resource(resource).await
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{aws::cloudwatch::CwClient, config::*};
    use regex::Regex;
    use rusoto_cloudwatch::CloudWatchClient;
    use rusoto_ec2::{Ec2Client, GroupIdentifier, Instance, InstanceState, Tag};
    use rusoto_mock::{MockCredentialsProvider, MockRequestDispatcher};

    static EC2_NAME_TAG: &str = "^ec2-(ue1|uw1|uw2|ew1|ec1|an1|an2|as1|as2|se1)-([1-2]{1})([a-c]{1})-(d|t|s|p)-([a-z0-9\\-]+)$";

    fn create_config() -> Ec2Config {
        Ec2Config {
            enabled: true,
            target_state: TargetState::Stopped,
            required_tags: None,
            allowed_instance_types: vec![],
            ignore: vec![],
            idle_rules: vec![],
            termination_protection: TerminationProtection { ignore: true },
            security_groups: SecurityGroups::default(),
            eni: Eni::default(),
            eip: Eip::default(),
        }
    }

    fn create_ec2_client(ec2_config: Ec2Config) -> Ec2Service {
        Ec2Service {
            client: Ec2Client::new_with(
                MockRequestDispatcher::default(),
                MockCredentialsProvider,
                Default::default(),
            ),
            cw_client: CwClient {
                client: CloudWatchClient::new_with(
                    MockRequestDispatcher::default(),
                    MockCredentialsProvider,
                    Default::default(),
                ),
                idle_rules: ec2_config.idle_rules.clone(),
            },
            config: ec2_config,
            region: Region::UsEast1,
            dry_run: true,
        }
    }

    fn get_ec2_instances() -> Vec<Instance> {
        vec![
            Instance {
                instance_id: Some("i-abc1234567".to_string()),
                instance_type: Some("d2.8xlarge".to_string()),
                state: Some(InstanceState {
                    code: Some(16),
                    ..Default::default()
                }),
                security_groups: Some(vec![GroupIdentifier {
                    group_id: Some("sg-6787980909".to_string()),
                    group_name: Some("some-group".to_string()),
                }]),
                ..Default::default()
            },
            Instance {
                instance_id: Some("i-def89012345".to_string()),
                instance_type: Some("t2.xlarge".to_string()),
                tags: Some(vec![Tag {
                    key: Some("Name".to_string()),
                    value: Some("ec2-uw2-1a-p-tbd".to_string()),
                }]),
                state: Some(InstanceState {
                    code: Some(16),
                    ..Default::default()
                }),
                security_groups: Some(vec![GroupIdentifier {
                    group_id: Some("sg-xxxxxxxx".to_string()),
                    group_name: Some("some-group".to_string()),
                }]),
                ..Default::default()
            },
            Instance {
                instance_id: Some("i-ghi89012345".to_string()),
                instance_type: Some("t2.2xlarge".to_string()),
                tags: Some(vec![
                    Tag {
                        key: Some("Name".to_string()),
                        value: Some("ec2-uw2-1a-p-tbd".to_string()),
                    },
                    Tag {
                        key: Some("Owner:Email".to_string()),
                        value: Some("abc@def.com".to_string()),
                    },
                ]),
                state: Some(InstanceState {
                    code: Some(16),
                    ..Default::default()
                }),
                security_groups: Some(vec![GroupIdentifier {
                    group_id: Some("sg-123456789".to_string()),
                    group_name: Some("some-group".to_string()),
                }]),
                ..Default::default()
            },
        ]
    }

    fn filter_resources(resources: Vec<Resource>) -> Vec<String> {
        resources
            .into_iter()
            .filter(|r| match r.enforcement_state {
                EnforcementState::Stop | EnforcementState::Delete => true,
                _ => false,
            })
            .map(|r| r.id)
            .collect()
    }

    #[tokio::test]
    async fn check_package_resources_by_single_tag() {
        let mut ec2_config = create_config();
        ec2_config.required_tags = Some(vec![RequiredTags {
            name: "Name".to_string(),
            pattern: Some(EC2_NAME_TAG.to_string()),
            regex: Some(Regex::new(EC2_NAME_TAG).unwrap()),
        }]);

        let ec2_client = create_ec2_client(ec2_config);
        let resources = ec2_client
            .package_instances_as_resources(get_ec2_instances(), None)
            .await
            .unwrap();

        let expected = vec!["i-abc1234567".to_string()];
        let result: Vec<String> = filter_resources(resources);

        assert_eq!(expected, result)
    }

    #[tokio::test]
    async fn check_package_resources_by_multiple_tags() {
        let mut ec2_config = create_config();
        ec2_config.required_tags = Some(vec![
            RequiredTags {
                name: "Name".to_string(),
                pattern: Some(EC2_NAME_TAG.to_string()),
                regex: Some(Regex::new(EC2_NAME_TAG).unwrap()),
            },
            RequiredTags {
                name: "Owner:Email".to_string(),
                pattern: Some("^(.*)@def.com".to_string()),
                regex: Some(Regex::new("^(.*)@def.com").unwrap()),
            },
        ]);

        let ec2_client = create_ec2_client(ec2_config);
        let resources = ec2_client
            .package_instances_as_resources(get_ec2_instances(), None)
            .await
            .unwrap();
        let expected = vec!["i-abc1234567".to_string(), "i-def89012345".to_string()];
        let result: Vec<String> = filter_resources(resources);

        assert_eq!(expected, result)
    }

    #[tokio::test]
    async fn check_package_resources_by_types() {
        let mut ec2_config = create_config();
        ec2_config.allowed_instance_types = vec!["t2.2xlarge".to_string(), "t2.xlarge".to_string()];

        let ec2_client = create_ec2_client(ec2_config);
        let resources = ec2_client
            .package_instances_as_resources(get_ec2_instances(), None)
            .await
            .unwrap();
        let expected = vec!["i-abc1234567".to_string()];
        let result: Vec<String> = filter_resources(resources);

        assert_eq!(expected, result)
    }

    #[tokio::test]
    async fn check_package_resources_by_sgs() {
        let mut ec2_config = create_config();
        ec2_config.security_groups.enabled = true;
        let open_sgs = vec!["sg-xxxxxxxx".to_string()];

        let ec2_client = create_ec2_client(ec2_config);
        let resources = ec2_client
            .package_instances_as_resources(get_ec2_instances(), Some(open_sgs))
            .await
            .unwrap();
        println!("{:?}", resources);
        let expected = vec!["i-def89012345".to_string()];
        let result: Vec<String> = filter_resources(resources);

        assert_eq!(expected, result)
    }

    #[tokio::test]
    async fn check_packaged_resources_by_all() {
        let mut ec2_config = create_config();
        ec2_config.required_tags = Some(vec![
            RequiredTags {
                name: "Name".to_string(),
                pattern: Some(EC2_NAME_TAG.to_string()),
                regex: Some(Regex::new(EC2_NAME_TAG).unwrap()),
            },
            RequiredTags {
                name: "Owner:Email".to_string(),
                pattern: Some("^(.*)@def.com".to_string()),
                regex: Some(Regex::new("^(.*)@def.com").unwrap()),
            },
        ]);
        ec2_config.allowed_instance_types = vec!["t2.2xlarge".to_string(), "t2.xlarge".to_string()];
        ec2_config.security_groups.enabled = true;
        let open_sgs = vec!["sg-xxxxxxxx".to_string()];

        let ec2_client = create_ec2_client(ec2_config);
        let resources = ec2_client
            .package_instances_as_resources(get_ec2_instances(), Some(open_sgs))
            .await
            .unwrap();
        println!("{:?}", resources);
        let mut expected = vec!["i-def89012345".to_string(), "i-abc1234567".to_string()];
        let mut result: Vec<String> = filter_resources(resources);

        assert_eq!(expected.sort(), result.sort())
    }
}
