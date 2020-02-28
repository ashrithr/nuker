use {
    rusoto_ce::GetCostAndUsageError,
    rusoto_core::region::ParseRegionError,
    rusoto_core::request::TlsError,
    rusoto_core::RusotoError,
    rusoto_credential::CredentialsError,
    rusoto_ec2::{
        DeleteNetworkInterfaceError, DeleteSnapshotError, DeleteVolumeError,
        DescribeAddressesError, DescribeInstanceAttributeError, DescribeInstancesError,
        DescribeNetworkInterfacesError, DescribeSecurityGroupsError, DescribeSnapshotsError,
        DescribeVolumesError, ModifyInstanceAttributeError, ReleaseAddressError,
        StopInstancesError, TerminateInstancesError,
    },
    rusoto_emr::{
        DescribeClusterError, ListClustersError, ListInstancesError, SetTerminationProtectionError,
        TerminateJobFlowsError,
    },
    rusoto_rds::{
        DeleteDBClusterError, DeleteDBInstanceError, DescribeDBClustersError,
        DescribeDBInstancesError, ListTagsForResourceError, ModifyDBClusterError,
        ModifyDBInstanceError, StopDBClusterError, StopDBInstanceError,
    },
    rusoto_redshift::{DeleteClusterError, DescribeClustersError},
    rusoto_s3::{
        DeleteBucketError, DeleteObjectsError, GetBucketTaggingError, ListBucketsError,
        ListObjectVersionsError, ListObjectsV2Error,
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
    #[fail(display = "Issue deleting Volume: {}", error)]
    VolumeDelete {
        error: RusotoError<DeleteVolumeError>,
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
    #[fail(display = "Failed listing S3 bucket tags: {}", error)]
    GetBucketTags {
        error: RusotoError<GetBucketTaggingError>,
    },
    #[fail(display = "Failed parsing the region: {}", error)]
    RegionParseError { error: ParseRegionError },
    #[fail(display = "Cloudwatch Error: {}", error)]
    CloudWatchError { error: String },
    #[fail(display = "Issue querying Cost Explorer: {}", error)]
    CeError {
        error: RusotoError<GetCostAndUsageError>,
    },
    #[fail(display = "JSON Encoding/Decoding error: {}", error)]
    JsonError { error: serde_json::error::Error },
}
