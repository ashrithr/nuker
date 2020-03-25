use rusoto_autoscaling::{DeleteAutoScalingGroupError, DescribeAutoScalingGroupsError};
use rusoto_ce::GetCostAndUsageError;
use rusoto_core::{region::ParseRegionError, request::TlsError, RusotoError};
use rusoto_credential::CredentialsError;
use rusoto_ec2::{
    DeleteInternetGatewayError, DeleteNatGatewayError, DeleteNetworkAclError,
    DeleteNetworkInterfaceError, DeleteRouteTableError, DeleteSecurityGroupError,
    DeleteSnapshotError, DeleteSubnetError, DeleteVolumeError, DeleteVpcEndpointsError,
    DeleteVpcError, DeleteVpcPeeringConnectionError, DeleteVpnGatewayError, DescribeAddressesError,
    DescribeInstanceAttributeError, DescribeInstancesError, DescribeNetworkInterfacesError,
    DescribeSecurityGroupsError, DescribeSnapshotsError, DescribeVolumesError, DescribeVpcsError,
    DetachVolumeError, ModifyInstanceAttributeError, ReleaseAddressError, StopInstancesError,
    TerminateInstancesError,
};
use rusoto_elbv2::{DeleteLoadBalancerError, DescribeLoadBalancersError};
use rusoto_emr::{
    DescribeClusterError, ListClustersError, ListInstancesError, SetTerminationProtectionError,
    TerminateJobFlowsError,
};
use rusoto_es::{
    DeleteElasticsearchDomainError, DescribeElasticsearchDomainError, ListDomainNamesError,
};
use rusoto_glue::{DeleteDevEndpointError, GetDevEndpointsError, GetTagsError};
use rusoto_rds::{
    DeleteDBClusterError, DeleteDBInstanceError, DescribeDBClustersError, DescribeDBInstancesError,
    ListTagsForResourceError, ModifyDBClusterError, ModifyDBInstanceError, StopDBClusterError,
    StopDBInstanceError,
};
use rusoto_redshift::{DeleteClusterError, DescribeClustersError};
use rusoto_s3::{
    DeleteBucketError, DeleteObjectsError, GetBucketLocationError, GetBucketTaggingError,
    ListBucketsError, ListObjectVersionsError, ListObjectsV2Error, PutBucketVersioningError,
};
use rusoto_sagemaker::{
    DeleteNotebookInstanceError, ListNotebookInstancesError, ListTagsError,
    StopNotebookInstanceError,
};
use rusoto_sts::GetCallerIdentityError;

#[derive(Debug, Fail, From)]
pub enum Error {
    #[fail(display = "Issue with credentials provider: {}", error)]
    CredentialProvider { error: CredentialsError },
    #[fail(display = "Issue with TLS provider: {}", error)]
    HttpsConnector { error: TlsError },
    #[fail(display = "Issue describing Instances: {}", error)]
    InstanceDescribe {
        error: RusotoError<DescribeInstancesError>,
    },
    #[fail(display = "Issue describing Volumes: {}", error)]
    VolumesDescribe {
        error: RusotoError<DescribeVolumesError>,
    },
    #[fail(display = "Issue deleting Volume: {}", error)]
    VolumeDelete {
        error: RusotoError<DeleteVolumeError>,
    },
    #[fail(display = "Failed detaching Volume: {}", error)]
    VolumeDetach {
        error: RusotoError<DetachVolumeError>,
    },
    #[fail(display = "Issue describing Snapshots: {}", error)]
    SnapshotsDescribe {
        error: RusotoError<DescribeSnapshotsError>,
    },
    #[fail(display = "Issue deleting Snapshot: {}", error)]
    SnapshotDelete {
        error: RusotoError<DeleteSnapshotError>,
    },
    #[fail(display = "Issue describing Security Groups: {}", error)]
    SecurityGroupsDescribe {
        error: RusotoError<DescribeSecurityGroupsError>,
    },
    #[fail(display = "Issue describing Network Interfaces: {}", error)]
    InterfacesDescribe {
        error: RusotoError<DescribeNetworkInterfacesError>,
    },
    #[fail(display = "Issue deleting network interface: {}", error)]
    InterfaceDelete {
        error: RusotoError<DeleteNetworkInterfaceError>,
    },
    #[fail(display = "Issue describing Addresses: {}", error)]
    AddressesDescribe {
        error: RusotoError<DescribeAddressesError>,
    },
    #[fail(display = "Issue releasing address: {}", error)]
    AddressDelete {
        error: RusotoError<ReleaseAddressError>,
    },
    #[fail(display = "Issue describing VPCs: {}", error)]
    VpcsDescribe {
        error: RusotoError<DescribeVpcsError>,
    },
    #[fail(display = "Issue deleting VPC: {}", error)]
    VpcDelete { error: RusotoError<DeleteVpcError> },
    #[fail(display = "Issue deleting IGW: {}", error)]
    IgwDelete {
        error: RusotoError<DeleteInternetGatewayError>,
    },
    #[fail(display = "Issue deleting Subnet: {}", error)]
    SubnetDelete {
        error: RusotoError<DeleteSubnetError>,
    },
    #[fail(display = "Issue deleting Route Table: {}", error)]
    RouteTableDelete {
        error: RusotoError<DeleteRouteTableError>,
    },
    #[fail(display = "Issue deleting Network ACL: {}", error)]
    NetworkAclDelete {
        error: RusotoError<DeleteNetworkAclError>,
    },
    #[fail(display = "Issue deleting VPC Peering Connection: {}", error)]
    PeeringConnectionDelete {
        error: RusotoError<DeleteVpcPeeringConnectionError>,
    },
    #[fail(display = "Issue deleting VPC Endpoint Request: {}", error)]
    EndpointRequestDelete {
        error: RusotoError<DeleteVpcEndpointsError>,
    },
    #[fail(display = "Issue deleting NAT Gateway: {}", error)]
    NatGatewayDelete {
        error: RusotoError<DeleteNatGatewayError>,
    },
    #[fail(display = "Issue deleting Security Group: {}", error)]
    SecurityGroupError {
        error: RusotoError<DeleteSecurityGroupError>,
    },
    #[fail(display = "Issue deleting VPN Gateway: {}", error)]
    VpnGatewayError {
        error: RusotoError<DeleteVpnGatewayError>,
    },
    #[fail(display = "Issue describing DB Instances: {}", error)]
    DBInstanceDescribe {
        error: RusotoError<DescribeDBInstancesError>,
    },
    #[fail(display = "Issue describing DB Clusters: {}", error)]
    DBClusterDescribe {
        error: RusotoError<DescribeDBClustersError>,
    },
    #[fail(display = "Issue listing tags for resource: {}", error)]
    ListTagsForResource {
        error: RusotoError<ListTagsForResourceError>,
    },
    #[fail(display = "Issue describing Instance attribute: {}", error)]
    InstanceAttributeDescribe {
        error: RusotoError<DescribeInstanceAttributeError>,
    },
    #[fail(display = "Issue describing EMR Clusters: {}", error)]
    EmrClustersDescribe {
        error: RusotoError<ListClustersError>,
    },
    #[fail(display = "Issue describing EMR Cluster: {}", error)]
    EmrClusterDescribe {
        error: RusotoError<DescribeClusterError>,
    },
    #[fail(display = "Issue describing EMR Instances: {}", error)]
    EmrInstancesDescribe {
        error: RusotoError<ListInstancesError>,
    },
    #[fail(display = "Issue setting EMR Termination Protection: {}", error)]
    EmrTerminationProtection {
        error: RusotoError<SetTerminationProtectionError>,
    },
    #[fail(display = "Issue terminating EMR clusters: {}", error)]
    EmrTerminateClusters {
        error: RusotoError<TerminateJobFlowsError>,
    },
    #[fail(display = "Issue describing Redshift Clusters: {}", error)]
    RedshiftClusterDescribe {
        error: RusotoError<DescribeClustersError>,
    },
    #[fail(display = "Issue modifying Instance attribute: {}", error)]
    InstanceAttributeModify {
        error: RusotoError<ModifyInstanceAttributeError>,
    },
    #[fail(display = "Issue terminating instances: {}", error)]
    InstancesTerminate {
        error: RusotoError<TerminateInstancesError>,
    },
    #[fail(display = "Issue stopping instances: {}", error)]
    InstancesStop {
        error: RusotoError<StopInstancesError>,
    },
    #[fail(display = "Failed modifying DB instance attribute: {}", error)]
    ModifyDBInstance {
        error: RusotoError<ModifyDBInstanceError>,
    },
    #[fail(display = "Failed stopping DB instance: {}", error)]
    StopDBInstance {
        error: RusotoError<StopDBInstanceError>,
    },
    #[fail(display = "Failed deleting DB instance: {}", error)]
    DeleteDBInstance {
        error: RusotoError<DeleteDBInstanceError>,
    },
    #[fail(display = "Failed modifying DB cluster: {}", error)]
    ModifyDBCluster {
        error: RusotoError<ModifyDBClusterError>,
    },
    #[fail(display = "Failed stopping DB cluster: {}", error)]
    StopDBCluster {
        error: RusotoError<StopDBClusterError>,
    },
    #[fail(display = "Failed deleting DB cluster: {}", error)]
    DeleteDBCluster {
        error: RusotoError<DeleteDBClusterError>,
    },
    #[fail(display = "Failed deleting Redshift cluster: {}", error)]
    DeleteRedshiftCluster {
        error: RusotoError<DeleteClusterError>,
    },
    #[fail(display = "Issue describing Buckets: {}", error)]
    DescribeS3Buckets {
        error: RusotoError<ListBucketsError>,
    },
    #[fail(display = "Issue describing Objects: {}", error)]
    DescribeS3Objects {
        error: RusotoError<ListObjectsV2Error>,
    },
    #[fail(display = "Issue describing Object Versions: {}", error)]
    DescribeS3ObjectVersions {
        error: RusotoError<ListObjectVersionsError>,
    },
    #[fail(display = "Failed deleting S3 objects: {}", error)]
    DeleteS3Objects {
        error: RusotoError<DeleteObjectsError>,
    },
    #[fail(display = "Failed deleting S3 bucket: {}", error)]
    DeleteS3Bucket {
        error: RusotoError<DeleteBucketError>,
    },
    #[fail(display = "Issue getting bucket location: {}", error)]
    GetS3BucketLocation {
        error: RusotoError<GetBucketLocationError>,
    },
    #[fail(display = "Failed listing S3 bucket tags: {}", error)]
    GetBucketTags {
        error: RusotoError<GetBucketTaggingError>,
    },
    #[fail(display = "Failed suspending versioning: {}", error)]
    DisableBucketVersioning {
        error: RusotoError<PutBucketVersioningError>,
    },
    #[fail(display = "Failed parsing the region: {}", error)]
    RegionParseError { error: ParseRegionError },
    #[fail(display = "Issue querying Cost Explorer: {}", error)]
    CeError {
        error: RusotoError<GetCostAndUsageError>,
    },
    #[fail(display = "Issue describing Glue Dev Endpoints: {}", error)]
    DescribeDevEndpoints {
        error: RusotoError<GetDevEndpointsError>,
    },
    #[fail(display = "Issue describing Glue Tags: {}", error)]
    DescribeTags { error: RusotoError<GetTagsError> },
    #[fail(display = "Issue getting caller identity: {}", error)]
    GetCallerIdentity {
        error: RusotoError<GetCallerIdentityError>,
    },
    #[fail(display = "Failed deleting Glue Dev Endpoint: {}", error)]
    DeleteDevEndpoint {
        error: RusotoError<DeleteDevEndpointError>,
    },
    #[fail(display = "Issue describing Sagemaker Notebooks: {}", error)]
    DescribeNotebooks {
        error: RusotoError<ListNotebookInstancesError>,
    },
    #[fail(display = "Issue describing Sagemaker Notebook tags: {}", error)]
    DescribeNotebookTags { error: RusotoError<ListTagsError> },
    #[fail(display = "Failed deleting Sagemaker Notebook: {}", error)]
    DeleteSagemakerNotebook {
        error: RusotoError<DeleteNotebookInstanceError>,
    },
    #[fail(display = "Failed stopping Sagemaker Notebook: {}", error)]
    StopSagemakerNotebook {
        error: RusotoError<StopNotebookInstanceError>,
    },
    #[fail(display = "Failed listing Elastic Domains: {}", error)]
    DescribeElasticDomains {
        error: RusotoError<ListDomainNamesError>,
    },
    #[fail(display = "Failed describing Elastic domain config: {}", error)]
    DescribeElasticDomain {
        error: RusotoError<DescribeElasticsearchDomainError>,
    },
    #[fail(display = "Failed describing tags for Elastic domain: {}", error)]
    DescribeElasticTags {
        error: RusotoError<rusoto_es::ListTagsError>,
    },
    #[fail(display = "Failed deleting Elastic domain: {}", error)]
    DeleteElasticDomain {
        error: RusotoError<DeleteElasticsearchDomainError>,
    },
    #[fail(display = "Failed describing tags for ELB: {}", error)]
    DescribeElbTags {
        error: RusotoError<rusoto_elbv2::DescribeTagsError>,
    },
    #[fail(display = "Failed describing load balancers: {}", error)]
    DescribeElbs {
        error: RusotoError<DescribeLoadBalancersError>,
    },
    #[fail(display = "Failed deleting load balancer: {}", error)]
    DeleteElb {
        error: RusotoError<DeleteLoadBalancerError>,
    },
    #[fail(display = "Issue describing AutoScaling Groups: {}", error)]
    DescribeAsg {
        error: RusotoError<DescribeAutoScalingGroupsError>,
    },
    #[fail(display = "Failed deleting AutoScaling Group: {}", error)]
    DeleteAsg {
        error: RusotoError<DeleteAutoScalingGroupError>,
    },
    #[fail(display = "Failed listing tags for ECS Cluster: {}", error)]
    ListEcsTags {
        error: RusotoError<rusoto_ecs::ListTagsForResourceError>,
    },
    #[fail(display = "Issue describing ECS clusters: {}", error)]
    DescribeEcsClusters {
        error: RusotoError<rusoto_ecs::ListClustersError>,
    },
    #[fail(display = "Issue describing ECS cluster: {}", error)]
    DescribeEcsCluster {
        error: RusotoError<rusoto_ecs::DescribeClustersError>,
    },
    #[fail(display = "Failed deleting ECS Cluster: {}", error)]
    DeleteEcsCluster {
        error: RusotoError<rusoto_ecs::DeleteClusterError>,
    },
    #[fail(display = "Failed listing ECS Cluster attributes: {}", error)]
    ListEcsAttributes {
        error: RusotoError<rusoto_ecs::ListAttributesError>,
    },
    #[fail(display = "Failed de-registering container instances: {}", error)]
    DeregisterEcsInstances {
        error: RusotoError<rusoto_ecs::DeregisterContainerInstanceError>,
    },
    #[fail(display = "Failed listing container instances: {}", error)]
    ListEcsInstances {
        error: RusotoError<rusoto_ecs::ListContainerInstancesError>,
    },
    #[fail(display = "JSON Encoding/Decoding error: {}", error)]
    JsonError { error: serde_json::error::Error },
    #[fail(display = "Encountered Tokio IO error: {}", error)]
    TokioError { error: tokio::io::Error },
    #[fail(display = "Internal Error: {}", error)]
    Internal { error: String },
}
