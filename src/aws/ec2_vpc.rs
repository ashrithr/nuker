use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::Result;
use crate::{handle_future, handle_future_with_return};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_ec2::{
    DeleteVpcRequest, DescribeInstancesRequest, DescribeInternetGatewaysRequest,
    DescribeNatGatewaysRequest, DescribeNetworkAclsRequest, DescribeNetworkInterfacesRequest,
    DescribeRouteTablesRequest, DescribeSecurityGroupsRequest, DescribeSubnetsRequest,
    DescribeVpcEndpointsRequest, DescribeVpcPeeringConnectionsRequest, DescribeVpcsRequest,
    DescribeVpnGatewaysRequest, Ec2, Ec2Client, Filter, Tag, Vpc,
};
use std::str::FromStr;
use tracing::{debug, trace};

#[derive(Clone)]
pub struct Ec2VpcClient {
    client: Ec2Client,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl Ec2VpcClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        Ec2VpcClient {
            client: Ec2Client::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn package_resources(&self, vpcs: Vec<Vpc>) -> Result<Vec<Resource>> {
        let mut resources: Vec<Resource> = Vec::new();

        for vpc in vpcs {
            let vpc_id = vpc.vpc_id.as_ref().unwrap();
            let arn = format!(
                "arn:aws:ec2:{}:{}:vpc/{}",
                self.region.name(),
                self.account_num,
                vpc_id
            );

            let enforcement_state: EnforcementState = {
                if vpc.is_default == Some(true) {
                    debug!(resource = &vpc_id[..], "Skipping default VPC");
                    EnforcementState::Skip
                } else {
                    EnforcementState::SkipUnknownState
                }
            };

            resources.push(Resource {
                id: vpc_id.into(),
                arn: Some(arn),
                type_: ClientType::Ec2Vpc,
                region: self.region.clone(),
                tags: self.package_tags(vpc.tags),
                state: ResourceState::from_str(vpc.state.as_deref().unwrap()).ok(),
                start_time: None,
                enforcement_state,
                resource_type: None,
                dependencies: None,
                termination_protection: None,
            });
        }

        Ok(resources)
    }

    fn package_tags(&self, tags: Option<Vec<Tag>>) -> Option<Vec<NTag>> {
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
            let req = self.client.describe_vpcs(DescribeVpcsRequest {
                next_token,
                ..Default::default()
            });

            if let Ok(result) = handle_future_with_return!(req) {
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
            } else {
                break;
            }
        }

        Ok(vpcs)
    }

    async fn get_dependencies(&self, resource: &Resource) -> Result<Vec<Resource>> {
        let mut resources = Vec::new();
        let vpc_id = resource.id.as_str();

        // Associated IGW's
        match self
            .client
            .describe_internet_gateways(DescribeInternetGatewaysRequest {
                filters: Some(vec![Filter {
                    name: Some("attachment.vpc-id".to_string()),
                    values: Some(vec![vpc_id.into()]),
                }]),
                ..Default::default()
            })
            .await
        {
            Ok(result) => {
                if let Some(igws) = result.internet_gateways {
                    for igw in igws {
                        let arn = format!(
                            "arn:aws:ec2:{}:{}:internet-gateway/{}",
                            self.region.name(),
                            self.account_num,
                            igw.internet_gateway_id.as_ref().unwrap(),
                        );

                        resources.push(Resource {
                            id: igw.internet_gateway_id.unwrap(),
                            arn: Some(arn),
                            type_: ClientType::Ec2Igw,
                            region: self.region.clone(),
                            tags: self.package_tags(igw.tags),
                            state: None,
                            start_time: None,
                            enforcement_state: EnforcementState::Delete,
                            resource_type: None,
                            dependencies: None,
                            termination_protection: None,
                        });
                    }
                }
            }
            Err(_) => {}
        }

        // Associated Subnets
        match self
            .client
            .describe_subnets(DescribeSubnetsRequest {
                filters: Some(vec![Filter {
                    name: Some("vpc-id".to_string()),
                    values: Some(vec![vpc_id.into()]),
                }]),
                ..Default::default()
            })
            .await
        {
            Ok(result) => {
                if let Some(subnets) = result.subnets {
                    for subnet in subnets {
                        let arn = format!(
                            "arn:aws:ec2:{}:{}:subnet/{}",
                            self.region.name(),
                            self.account_num,
                            subnet.subnet_id.as_ref().unwrap(),
                        );

                        resources.push(Resource {
                            id: subnet.subnet_id.unwrap(),
                            arn: Some(arn),
                            type_: ClientType::Ec2Subnet,
                            region: self.region.clone(),
                            tags: self.package_tags(subnet.tags),
                            state: None,
                            start_time: None,
                            enforcement_state: EnforcementState::Delete,
                            resource_type: None,
                            dependencies: None,
                            termination_protection: None,
                        });
                    }
                }
            }
            Err(_) => {}
        }

        // Associated Route tables
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
                        if !rt
                            .associations
                            .unwrap_or_default()
                            .iter()
                            .any(|rta| rta.main == Some(true))
                        {
                            let arn = format!(
                                "arn:aws:ec2:{}:{}:route-table/{}",
                                self.region.name(),
                                self.account_num,
                                rt.route_table_id.as_ref().unwrap(),
                            );

                            resources.push(Resource {
                                id: rt.route_table_id.unwrap(),
                                arn: Some(arn),
                                type_: ClientType::Ec2RouteTable,
                                region: self.region.clone(),
                                tags: self.package_tags(rt.tags),
                                state: None,
                                start_time: None,
                                enforcement_state: EnforcementState::Delete,
                                resource_type: None,
                                dependencies: None,
                                termination_protection: None,
                            });
                        }
                    }
                }
            }
            Err(_) => {}
        }

        // Associated NACLs
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
                        if nacl.is_default != Some(true) {
                            let arn = format!(
                                "arn:aws:ec2:{}:{}:network-acl/{}",
                                self.region.name(),
                                self.account_num,
                                nacl.network_acl_id.as_ref().unwrap(),
                            );

                            resources.push(Resource {
                                id: nacl.network_acl_id.unwrap(),
                                arn: Some(arn),
                                type_: ClientType::Ec2NetworkACL,
                                region: self.region.clone(),
                                tags: self.package_tags(nacl.tags),
                                state: None,
                                start_time: None,
                                enforcement_state: EnforcementState::Delete,
                                resource_type: None,
                                dependencies: None,
                                termination_protection: None,
                            });
                        }
                    }
                }
            }
            Err(_) => {}
        }

        // Associated VPC Peering Connections
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
                        let arn = format!(
                            "arn:aws:ec2:{}:{}:vpc-peering-connection/{}",
                            self.region.name(),
                            self.account_num,
                            conn.vpc_peering_connection_id.as_ref().unwrap(),
                        );

                        resources.push(Resource {
                            id: conn.vpc_peering_connection_id.unwrap(),
                            arn: Some(arn),
                            type_: ClientType::Ec2PeeringConnection,
                            region: self.region.clone(),
                            tags: self.package_tags(conn.tags),
                            state: None,
                            start_time: None,
                            enforcement_state: EnforcementState::Delete,
                            resource_type: None,
                            dependencies: None,
                            termination_protection: None,
                        });
                    }
                }
            }
            Err(_) => {}
        }

        // Associated VPC Endpoints
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
                    for endpoint in endpoints {
                        let arn = format!(
                            "arn:aws:ec2:{}:{}:vpc-endpoint/{}",
                            self.region.name(),
                            self.account_num,
                            endpoint.vpc_endpoint_id.as_ref().unwrap(),
                        );

                        resources.push(Resource {
                            id: endpoint.vpc_endpoint_id.unwrap(),
                            arn: Some(arn),
                            type_: ClientType::Ec2VpcEndpoint,
                            region: self.region.clone(),
                            tags: self.package_tags(endpoint.tags),
                            state: None,
                            start_time: None,
                            enforcement_state: EnforcementState::Delete,
                            resource_type: None,
                            dependencies: None,
                            termination_protection: None,
                        });
                    }
                }
            }
            Err(_) => {}
        }

        // Associated NAT Gateways
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
                        resources.push(Resource {
                            id: nat.nat_gateway_id.unwrap(),
                            arn: None,
                            type_: ClientType::Ec2NatGW,
                            region: self.region.clone(),
                            tags: self.package_tags(nat.tags),
                            state: None,
                            start_time: None,
                            enforcement_state: EnforcementState::Delete,
                            resource_type: None,
                            dependencies: None,
                            termination_protection: None,
                        });
                    }
                }
            }
            Err(_) => {}
        }

        // Associated Security Groups
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
                        if sg.group_name.as_ref().map(|s| s.as_str()) != Some("default") {
                            let arn = format!(
                                "arn:aws:ec2:{}:{}:security-group/{}",
                                self.region.name(),
                                self.account_num,
                                sg.group_id.as_ref().unwrap(),
                            );

                            resources.push(Resource {
                                id: sg.group_id.unwrap(),
                                arn: Some(arn),
                                type_: ClientType::Ec2Sg,
                                region: self.region.clone(),
                                tags: self.package_tags(sg.tags),
                                state: None,
                                start_time: None,
                                enforcement_state: EnforcementState::Delete,
                                resource_type: None,
                                dependencies: None,
                                termination_protection: None,
                            });
                        }
                    }
                }
            }
            Err(_) => {}
        }

        // Instances
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
                            for instance in instances {
                                let arn = format!(
                                    "arn:aws:ec2:{}:{}:instance/{}",
                                    self.region.name(),
                                    self.account_num,
                                    instance.instance_id.as_ref().unwrap(),
                                );

                                resources.push(Resource {
                                    id: instance.instance_id.unwrap(),
                                    arn: Some(arn),
                                    type_: ClientType::Ec2Instance,
                                    region: self.region.clone(),
                                    tags: self.package_tags(instance.tags),
                                    state: None,
                                    start_time: None,
                                    enforcement_state: EnforcementState::Delete,
                                    resource_type: None,
                                    dependencies: None,
                                    termination_protection: None,
                                });
                            }
                        }
                    }
                }
            }
            Err(_) => {}
        }

        // VPN Gateways
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
                        let arn = format!(
                            "arn:aws:ec2:{}:{}:vpn-gateway/{}",
                            self.region.name(),
                            self.account_num,
                            vpn_gateway.vpn_gateway_id.as_ref().unwrap(),
                        );

                        resources.push(Resource {
                            id: vpn_gateway.vpn_gateway_id.unwrap(),
                            arn: Some(arn),
                            type_: ClientType::Ec2VpnGW,
                            region: self.region.clone(),
                            tags: self.package_tags(vpn_gateway.tags),
                            state: None,
                            start_time: None,
                            enforcement_state: EnforcementState::Delete,
                            resource_type: None,
                            dependencies: None,
                            termination_protection: None,
                        });
                    }
                }
            }
            Err(_) => {}
        }

        // Network Interfaces
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
                        let arn = format!(
                            "arn:aws:ec2:{}:{}:network-interface/{}",
                            self.region.name(),
                            self.account_num,
                            network_interface.network_interface_id.as_ref().unwrap(),
                        );

                        resources.push(Resource {
                            id: network_interface.network_interface_id.unwrap(),
                            arn: Some(arn),
                            type_: ClientType::Ec2Eni,
                            region: self.region.clone(),
                            tags: self.package_tags(network_interface.tag_set),
                            state: None,
                            start_time: None,
                            enforcement_state: EnforcementState::Delete,
                            resource_type: None,
                            dependencies: None,
                            termination_protection: None,
                        });
                    }
                }
            }
            Err(_) => {}
        }

        Ok(resources)
    }

    async fn delete_vpc(&self, resource: &Resource) -> Result<()> {
        debug!(resource = resource.id.as_str(), "Deleting");

        if !self.dry_run {
            let req = self.client.delete_vpc(DeleteVpcRequest {
                vpc_id: resource.id.to_string(),
                ..Default::default()
            });
            handle_future!(req);
        }

        Ok(())
    }
}

#[async_trait]
impl NukerClient for Ec2VpcClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Initialized VPC resource scanner");
        let vpcs = self.get_vpcs().await?;

        Ok(self.package_resources(vpcs).await?)
    }

    async fn dependencies(&self, resource: &Resource) -> Option<Vec<Resource>> {
        self.get_dependencies(resource).await.ok()
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
        // match resource.resource_type {
        //     ResourceType::Vpc => self.delete_vpc(resource).await,
        //     ResourceType::VpcIgw => self.delete_igw(resource).await,
        //     ResourceType::VpcEndpoint => self.delete_endpoint(resource).await,
        //     ResourceType::VpcNacl => self.delete_nacl(resource).await,
        //     ResourceType::VpcNatGw => self.delete_nat_gateway(resource).await,
        //     ResourceType::VpcPeerConn => self.delete_peer_conn(resource).await,
        //     ResourceType::VpcRt => self.delete_rt(resource).await,
        //     ResourceType::VpcSubnet => self.delete_subnet(resource).await,
        //     ResourceType::VpcVpnGw => self.delete_vpn_gateway(resource).await,
        //     _ => Ok(()),
        // }
        self.delete_vpc(resource).await
    }
}
