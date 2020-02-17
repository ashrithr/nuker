use {
    rusoto_ce::GetCostAndUsageError,
    rusoto_core::region::ParseRegionError,
    rusoto_core::request::TlsError,
    rusoto_core::RusotoError,
    rusoto_credential::CredentialsError,
    rusoto_ec2::{
        DescribeInstanceAttributeError, DescribeInstancesError, DescribeSecurityGroupsError,
        DescribeVolumesError, ModifyInstanceAttributeError, StopInstancesError,
        TerminateInstancesError,
    },
    rusoto_pricing::{DescribeServicesError, GetProductsError},
    rusoto_rds::{
        DeleteDBClusterError, DeleteDBInstanceError, DescribeDBClustersError,
        DescribeDBInstancesError, ListTagsForResourceError, ModifyDBClusterError,
        ModifyDBInstanceError, StopDBClusterError, StopDBInstanceError,
    },
};

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
    #[fail(display = "Issue describing Security Groups: {}", error)]
    SecurityGroupsDescribe {
        error: RusotoError<DescribeSecurityGroupsError>,
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
    #[fail(display = "Failed parsing the region: {}", error)]
    RegionParseError { error: ParseRegionError },
    #[fail(display = "Cloudwatch Error: {}", error)]
    CloudWatchError { error: String },
    #[fail(display = "Issue querying Cost Explorer: {}", error)]
    CeError {
        error: RusotoError<GetCostAndUsageError>,
    },
    #[fail(display = "Issue querying Pricing: {}", error)]
    DescribeServicesError {
        error: RusotoError<DescribeServicesError>,
    },
    #[fail(display = "Issue querying Pricing: {}", error)]
    GetProductsError {
        error: RusotoError<GetProductsError>,
    },
    #[fail(display = "JSON Encoding/Decoding error: {}", error)]
    JsonError { error: serde_json::error::Error },
}
