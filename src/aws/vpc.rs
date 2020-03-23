use crate::{
    aws::{util, Result},
    config::{RequiredTags, VpcConfig},
    resource::{EnforcementState, NTag, Resource, ResourceType},
    service::NukerService,
};
use async_trait::async_trait;
use rusoto_core::{credential::ProfileProvider, HttpClient, Region};
use rusoto_ec2::{
    DeleteInternetGatewayRequest, DeleteNatGatewayRequest, DeleteNetworkAclRequest,
    DeleteNetworkInterfaceRequest, DeleteRouteTableRequest, DeleteSecurityGroupRequest,
    DeleteSubnetRequest, DeleteVpcEndpointsRequest, DeleteVpcPeeringConnectionRequest,
    DeleteVpcRequest, DeleteVpnGatewayRequest, DescribeInstancesRequest,
    DescribeInternetGatewaysRequest, DescribeNatGatewaysRequest, DescribeNetworkAclsRequest,
    DescribeNetworkInterfacesRequest, DescribeRouteTablesRequest, DescribeSecurityGroupsRequest,
    DescribeSubnetsRequest, DescribeVpcEndpointsRequest, DescribeVpcPeeringConnectionsRequest,
    DescribeVpcsRequest, DescribeVpnGatewaysRequest, Ec2, Ec2Client, Filter, Tag,
    TerminateInstancesRequest, Vpc,
};
use tracing::{debug, trace};

#[derive(Clone)]
pub struct VpcService {
    pub client: Ec2Client,
    pub config: VpcConfig,
    pub region: Region,
    pub dry_run: bool,
}

impl VpcService {
    pub fn new(
        profile_name: Option<String>,
        region: Region,
        config: VpcConfig,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = &profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(VpcService {
                client: Ec2Client::new_with(HttpClient::new()?, pp, region.clone()),
                config,
                region,
                dry_run,
            })
        } else {
            Ok(VpcService {
                client: Ec2Client::new(region.clone()),
                config,
                region,
                dry_run,
            })
        }
    }

    async fn package_vpcs_as_resources(&self, vpcs: Vec<Vpc>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for vpc in vpcs {
            let vpc_id = vpc.vpc_id.as_ref().unwrap();

            let enforcement_state: EnforcementState = {
                if vpc.is_default == Some(true) {
                    debug!(resource = &vpc_id[..], "Skipping default VPC");
                    EnforcementState::Skip
                } else if self.config.ignore.contains(vpc_id) {
                    debug!(resource = &vpc_id[..], "Skipping resource from ignore list");
                    EnforcementState::SkipConfig
                } else {
                    if self.resource_tags_does_not_match(&vpc) {
                        debug!(resource = &vpc_id[..], "VPC tags does not match");
                        EnforcementState::Delete
                    } else {
                        EnforcementState::Skip
                    }
                }
            };

            resources.push(Resource {
                id: vpc_id.into(),
                arn: None,
                resource_type: ResourceType::Vpc,
                region: self.region.clone(),
                tags: self.package_tags_as_ntags(vpc.tags),
                state: vpc.state,
                enforcement_state,
                dependencies: None,
            });
        }

        Ok(resources)
    }

    fn resource_tags_does_not_match(&self, vpc: &Vpc) -> bool {
        if self.config.required_tags.is_some() {
            !self.check_tags(&vpc.tags, &self.config.required_tags.as_ref().unwrap())
        } else {
            false
        }
    }

    fn check_tags(&self, tags: &Option<Vec<Tag>>, required_tags: &Vec<RequiredTags>) -> bool {
        let ntags = self.package_tags_as_ntags(tags.to_owned());
        util::compare_tags(ntags, required_tags)
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

    async fn get_vpcs(&self) -> Result<Vec<Vpc>> {
        let mut vpcs: Vec<Vpc> = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let result = self
                .client
                .describe_vpcs(DescribeVpcsRequest {
                    next_token,
                    ..Default::default()
                })
                .await?;

            if let Some(vs) = result.vpcs {
                for v in vs {
                    vpcs.push(v);
                }
            }

            if result.next_token.is_none() {
                break;
            } else {
                next_token = result.next_token;
            }
        }

        Ok(vpcs)
    }

    /// Find and delete all dependencies before attempting to delete the VPC.
    ///
    /// FIXME: Track dependent resources as a DAG and use the dependency tree to cleanup the
    /// resources accordingly.
    async fn delete_dependencies(&self, vpc_id: &String) -> bool {
        // 1. Delete any associated IGW's
        match self
            .client
            .describe_internet_gateways(DescribeInternetGatewaysRequest {
                filters: Some(vec![Filter {
                    name: Some("attachment.vpc-id".to_string()),
                    values: Some(vec![vpc_id.to_owned()]),
                }]),
                ..Default::default()
            })
            .await
        {
            Ok(result) => {
                if let Some(igws) = result.internet_gateways {
                    for igw in igws {
                        let igw_id = igw.internet_gateway_id.unwrap();

                        match self
                            .client
                            .delete_internet_gateway(DeleteInternetGatewayRequest {
                                internet_gateway_id: igw_id,
                                ..Default::default()
                            })
                            .await
                        {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    }
                }
            }
            Err(_) => {}
        }

        // 2. Delete any associated Subnets
        match self
            .client
            .describe_subnets(DescribeSubnetsRequest {
                filters: Some(vec![Filter {
                    name: Some("vpc-id".to_string()),
                    values: Some(vec![vpc_id.to_owned()]),
                }]),
                ..Default::default()
            })
            .await
        {
            Ok(result) => {
                if let Some(subnets) = result.subnets {
                    for subnet in subnets {
                        let subnet_id = subnet.subnet_id.unwrap();

                        match self
                            .client
                            .delete_subnet(DeleteSubnetRequest {
                                subnet_id,
                                ..Default::default()
                            })
                            .await
                        {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    }
                }
            }
            Err(_) => {}
        }

        // 3. Delete any associated Route tables
        match self
            .client
            .describe_route_tables(DescribeRouteTablesRequest {
                filters: Some(vec![Filter {
                    name: Some("vpc-id".to_string()),
                    values: Some(vec![vpc_id.to_string()]),
                }]),
                ..Default::default()
            })
            .await
        {
            Ok(result) => {
                if let Some(rts) = result.route_tables {
                    for rt in rts {
                        // route_table_id
                        match self
                            .client
                            .delete_route_table(DeleteRouteTableRequest {
                                route_table_id: rt.route_table_id.unwrap(),
                                ..Default::default()
                            })
                            .await
                        {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    }
                }
            }
            Err(_) => {}
        }

        // 4. Delete any associated NACLs
        match self
            .client
            .describe_network_acls(DescribeNetworkAclsRequest {
                filters: Some(vec![Filter {
                    name: Some("vpc-id".to_string()),
                    values: Some(vec![vpc_id.to_string()]),
                }]),
                ..Default::default()
            })
            .await
        {
            Ok(result) => {
                if let Some(nacls) = result.network_acls {
                    for nacl in nacls {
                        match self
                            .client
                            .delete_network_acl(DeleteNetworkAclRequest {
                                network_acl_id: nacl.network_acl_id.unwrap(),
                                ..Default::default()
                            })
                            .await
                        {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    }
                }
            }
            Err(_) => {}
        }

        // 5. VPC Peering Connections
        match self
            .client
            .describe_vpc_peering_connections(DescribeVpcPeeringConnectionsRequest {
                filters: Some(vec![Filter {
                    name: Some("requester-vpc-info.vpc-id".to_string()),
                    values: Some(vec![vpc_id.to_string()]),
                }]),
                ..Default::default()
            })
            .await
        {
            Ok(result) => {
                if let Some(peering_connections) = result.vpc_peering_connections {
                    for conn in peering_connections {
                        match self
                            .client
                            .delete_vpc_peering_connection(DeleteVpcPeeringConnectionRequest {
                                vpc_peering_connection_id: conn.vpc_peering_connection_id.unwrap(),
                                ..Default::default()
                            })
                            .await
                        {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    }
                }
            }
            Err(_) => {}
        }

        // 6. VPC Endpoints
        match self
            .client
            .describe_vpc_endpoints(DescribeVpcEndpointsRequest {
                filters: Some(vec![Filter {
                    name: Some("vpc-id".to_string()),
                    values: Some(vec![vpc_id.to_string()]),
                }]),
                ..Default::default()
            })
            .await
        {
            Ok(result) => {
                if let Some(endpoints) = result.vpc_endpoints {
                    let vpc_endpoint_ids = endpoints
                        .into_iter()
                        .map(|e| e.vpc_endpoint_id.unwrap())
                        .collect::<Vec<String>>();

                    match self
                        .client
                        .delete_vpc_endpoints(DeleteVpcEndpointsRequest {
                            vpc_endpoint_ids,
                            ..Default::default()
                        })
                        .await
                    {
                        Ok(_) => {}
                        Err(_) => {}
                    }
                }
            }
            Err(_) => {}
        }

        // 7. NAT Gateways
        match self
            .client
            .describe_nat_gateways(DescribeNatGatewaysRequest {
                filter: Some(vec![Filter {
                    name: Some("vpc-id".to_string()),
                    values: Some(vec![vpc_id.to_string()]),
                }]),
                ..Default::default()
            })
            .await
        {
            Ok(result) => {
                if let Some(nat_gateways) = result.nat_gateways {
                    for nat in nat_gateways {
                        match self
                            .client
                            .delete_nat_gateway(DeleteNatGatewayRequest {
                                nat_gateway_id: nat.nat_gateway_id.unwrap(),
                            })
                            .await
                        {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    }
                }
            }
            Err(_) => {}
        }

        // 8. Security Groups
        match self
            .client
            .describe_security_groups(DescribeSecurityGroupsRequest {
                filters: Some(vec![Filter {
                    name: Some("vpc-id".to_string()),
                    values: Some(vec![vpc_id.to_string()]),
                }]),
                ..Default::default()
            })
            .await
        {
            Ok(result) => {
                if let Some(security_groups) = result.security_groups {
                    for sg in security_groups {
                        match self
                            .client
                            .delete_security_group(DeleteSecurityGroupRequest {
                                group_id: sg.group_id,
                                ..Default::default()
                            })
                            .await
                        {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    }
                }
            }
            Err(_) => {}
        }

        // 9. Instances
        match self
            .client
            .describe_instances(DescribeInstancesRequest {
                filters: Some(vec![Filter {
                    name: Some("vpc-id".to_string()),
                    values: Some(vec![vpc_id.to_string()]),
                }]),
                ..Default::default()
            })
            .await
        {
            Ok(result) => {
                if let Some(reservations) = result.reservations {
                    for reservation in reservations {
                        if let Some(instances) = reservation.instances {
                            let instance_ids = instances
                                .into_iter()
                                .map(|i| i.instance_id.unwrap())
                                .collect::<Vec<String>>();

                            match self
                                .client
                                .terminate_instances(TerminateInstancesRequest {
                                    instance_ids,
                                    ..Default::default()
                                })
                                .await
                            {
                                Ok(_) => {}
                                Err(_) => {}
                            }
                        }
                    }
                }
            }
            Err(_) => {}
        }

        // 10. VPN Gateways
        match self
            .client
            .describe_vpn_gateways(DescribeVpnGatewaysRequest {
                filters: Some(vec![Filter {
                    name: Some("attachment.vpc-id".to_string()),
                    values: Some(vec![vpc_id.to_string()]),
                }]),
                ..Default::default()
            })
            .await
        {
            Ok(result) => {
                if let Some(vpn_gateways) = result.vpn_gateways {
                    for vpn_gateway in vpn_gateways {
                        match self
                            .client
                            .delete_vpn_gateway(DeleteVpnGatewayRequest {
                                vpn_gateway_id: vpn_gateway.vpn_gateway_id.unwrap(),
                                ..Default::default()
                            })
                            .await
                        {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    }
                }
            }
            Err(_) => {}
        }

        // 11. Network Interfaces
        match self
            .client
            .describe_network_interfaces(DescribeNetworkInterfacesRequest {
                filters: Some(vec![Filter {
                    name: Some("vpc-id".to_string()),
                    values: Some(vec![vpc_id.to_string()]),
                }]),
                ..Default::default()
            })
            .await
        {
            Ok(result) => {
                if let Some(network_interfaces) = result.network_interfaces {
                    for network_interface in network_interfaces {
                        match self
                            .client
                            .delete_network_interface(DeleteNetworkInterfaceRequest {
                                network_interface_id: network_interface
                                    .network_interface_id
                                    .unwrap(),
                                ..Default::default()
                            })
                            .await
                        {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    }
                }
            }
            Err(_) => {}
        }

        false
    }

    async fn delete_resource(&self, resource: &Resource) -> Result<()> {
        if !self.dry_run {
            self.client
                .delete_vpc(DeleteVpcRequest {
                    vpc_id: resource.id.to_string(),
                    ..Default::default()
                })
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl NukerService for VpcService {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized VPC resource scanner");
        let vpcs = self.get_vpcs().await?;

        Ok(self.package_vpcs_as_resources(vpcs).await?)
    }

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.delete_resource(resource).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_resource(resource).await
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}
