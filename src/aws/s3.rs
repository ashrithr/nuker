use crate::{
    aws::Result,
    config::S3Config,
    resource::{EnforcementState, NTag, Resource, ResourceType},
    service::NukerService,
};
use async_trait::async_trait;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use rusoto_s3::{
    Bucket, Delete, DeleteBucketRequest, DeleteObjectsRequest, GetBucketAclRequest,
    GetBucketLocationRequest, GetBucketPolicyStatusRequest, GetBucketTaggingRequest,
    GetPublicAccessBlockRequest, Grant, ListObjectVersionsRequest, ListObjectsV2Request,
    ObjectIdentifier, PolicyStatus, PublicAccessBlockConfiguration, S3Client, Tag, S3,
};
use tracing::{debug, trace};

static S3_PUBLIC_GROUPS: [&str; 2] = [
    "http://acs.amazonaws.com/groups/global/AuthenticatedUsers",
    "http://acs.amazonaws.com/groups/global/AllUsers",
];

#[derive(Clone)]
pub struct S3Service {
    client: S3Client,
    config: S3Config,
    region: Region,
    dry_run: bool,
}

impl S3Service {
    pub fn new(
        profile_name: Option<String>,
        region: Region,
        config: S3Config,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = &profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(S3Service {
                client: S3Client::new_with(HttpClient::new()?, pp, region.clone()),
                config,
                region,
                dry_run,
            })
        } else {
            Ok(S3Service {
                client: S3Client::new(region.clone()),
                config,
                region,
                dry_run,
            })
        }
    }

    async fn package_buckets_as_resources(&self, buckets: Vec<Bucket>) -> Vec<Resource> {
        let mut resources: Vec<Resource> = Vec::new();

        for bucket in buckets {
            let bucket_id = bucket.name.unwrap();

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&bucket_id) {
                    EnforcementState::SkipConfig
                } else if self.resource_prefix_does_not_match(&bucket_id) {
                    debug!(
                        resource = bucket_id.as_str(),
                        "Bucket prefix does not match"
                    );
                    EnforcementState::Delete
                } else if self.resource_name_is_not_dns_compliant(&bucket_id) {
                    debug!(
                        resource = bucket_id.as_str(),
                        "Bucket name is not dns compliant"
                    );
                    EnforcementState::Delete
                } else if self.is_bucket_public(&bucket_id).await {
                    debug!(
                        resource = bucket_id.as_str(),
                        "Bucket is publicly accessible"
                    );
                    EnforcementState::Delete
                } else {
                    EnforcementState::Skip
                }
            };

            resources.push(Resource {
                id: bucket_id.clone(),
                arn: None,
                region: self.region.clone(),
                resource_type: ResourceType::S3Bucket,
                tags: self.package_tags_as_ntags(self.get_tags_for_bucket(&bucket_id).await),
                state: Some("Available".to_string()),
                enforcement_state,
                dependencies: None,
            })
        }

        resources
    }

    fn resource_prefix_does_not_match(&self, bucket_id: &str) -> bool {
        if self.config.required_naming_prefix.is_some() {
            !self
                .config
                .required_naming_regex
                .as_ref()
                .unwrap()
                .is_match(bucket_id)
        } else {
            false
        }
    }

    fn resource_name_is_not_dns_compliant(&self, bucket_id: &str) -> bool {
        if self.config.check_dns_compliant_naming.is_some() {
            bucket_id.contains('.')
        } else {
            false
        }
    }

    async fn is_bucket_public(&self, bucket_id: &str) -> bool {
        // let mut pbc_is_public = false;
        let mut ps_is_public = false;
        let mut grants_is_public = false;

        if self.config.check_public_accessibility.is_some() {
            // 1. Check the Public Access Block
            // if let Some(public_access_block) = self.get_public_access_block(bucket_id) {
            //     if public_access_block.block_public_acls == Some(false)
            //         && public_access_block.block_public_policy == Some(false)
            //     {
            //         debug!("Bucket ({}) has open public access block", bucket_id);
            //         pbc_is_public = true;
            //     }
            // }

            // 2. Check the Bucket Policy Status
            if let Some(policy_status) = self.get_bucket_policy_status(bucket_id).await {
                if policy_status.is_public == Some(true) {
                    debug!("Bucket ({}) policy status is public", bucket_id);
                    ps_is_public = true;
                }
            }

            // 3. Check the ACLs to see if grantee is AllUsers or AllAuthenticatedUsers
            // TODO: https://github.com/rusoto/rusoto/issues/1703
            if let Some(grants) = self.get_acls_for_bucket(bucket_id).await {
                debug!("Bucket ({}) grants - {:?}", bucket_id, grants);
                for grant in grants {
                    if grant.grantee.is_some() && grant.permission.is_some() {
                        let grantee = grant.grantee.as_ref().unwrap().clone();

                        if grantee.type_ == "Group".to_string()
                            && S3_PUBLIC_GROUPS.contains(&grantee.uri.unwrap().as_str())
                        {
                            debug!("Bucket ({}) grants are public", bucket_id);
                            grants_is_public = true;
                        }
                    }
                }
            }

            if ps_is_public || grants_is_public {
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    async fn get_buckets(&self) -> Result<Vec<Bucket>> {
        let result = self.client.list_buckets().await?;
        let mut buckets: Vec<Bucket> = Vec::new();

        for bucket in result.buckets.unwrap_or_default() {
            let bucket_loc_res = self
                .client
                .get_bucket_location(GetBucketLocationRequest {
                    bucket: bucket.name.as_ref().unwrap().to_string(),
                })
                .await?;
            let bucket_loc = if !bucket_loc_res
                .location_constraint
                .as_ref()
                .unwrap()
                .is_empty()
            {
                bucket_loc_res
                    .location_constraint
                    .as_ref()
                    .unwrap()
                    .as_str()
            } else {
                Region::UsEast1.name()
            };
            let region = self.region.name();

            if bucket_loc == region {
                buckets.push(bucket);
            }
        }

        Ok(buckets)
    }

    async fn get_acls_for_bucket(&self, bucket_id: &str) -> Option<Vec<Grant>> {
        match self
            .client
            .get_bucket_acl(GetBucketAclRequest {
                bucket: bucket_id.to_string(),
            })
            .await
        {
            Ok(result) => result.grants,
            Err(err) => {
                trace!(
                    "Failed to get ACL Grants for bucket {} - {:?}",
                    bucket_id,
                    err
                );

                None
            }
        }
    }

    #[allow(dead_code)]
    async fn get_public_access_block(
        &self,
        bucket_id: &str,
    ) -> Option<PublicAccessBlockConfiguration> {
        match self
            .client
            .get_public_access_block(GetPublicAccessBlockRequest {
                bucket: bucket_id.to_string(),
            })
            .await
        {
            Ok(result) => result.public_access_block_configuration,
            Err(err) => {
                trace!(
                    "Failed to get public_access_block for bucket {} - {:?}",
                    bucket_id,
                    err
                );

                None
            }
        }
    }

    async fn get_bucket_policy_status(&self, bucket_id: &str) -> Option<PolicyStatus> {
        match self
            .client
            .get_bucket_policy_status(GetBucketPolicyStatusRequest {
                bucket: bucket_id.to_string(),
            })
            .await
        {
            Ok(result) => result.policy_status,
            Err(err) => {
                trace!(
                    "Failed to get bucket policy status for bucket {} - {:?}",
                    bucket_id,
                    err
                );

                None
            }
        }
    }

    async fn delete_objects_in_bucket(&self, bucket: &str) -> Result<()> {
        let mut next_token: Option<String> = None;

        // Delete all objects from the bucket
        loop {
            let result = self
                .client
                .list_objects_v2(ListObjectsV2Request {
                    bucket: bucket.to_owned(),
                    continuation_token: next_token,
                    ..Default::default()
                })
                .await?;

            if let Some(objects) = result.contents {
                self.client
                    .delete_objects(DeleteObjectsRequest {
                        bucket: bucket.to_owned(),
                        bypass_governance_retention: Some(true),
                        delete: Delete {
                            objects: objects
                                .iter()
                                .map(|o| ObjectIdentifier {
                                    key: o.key.as_ref().unwrap().to_owned(),
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        },
                        ..Default::default()
                    })
                    .await?;
            }

            if result.next_continuation_token.is_none() {
                break;
            } else {
                next_token = result.next_continuation_token;
            }
        }

        Ok(())
    }

    async fn delete_versions_in_bucket(&self, bucket: &str) -> Result<()> {
        // Delete all object versions(required for versioned buckets)
        let mut next_key_token: Option<String> = None;
        let mut next_version_token: Option<String> = None;
        loop {
            let result = self
                .client
                .list_object_versions(ListObjectVersionsRequest {
                    bucket: bucket.to_owned(),
                    key_marker: next_key_token,
                    version_id_marker: next_version_token,
                    ..Default::default()
                })
                .await?;

            if let Some(versions) = result.versions {
                self.client
                    .delete_objects(DeleteObjectsRequest {
                        bucket: bucket.to_owned(),
                        bypass_governance_retention: Some(true),
                        delete: Delete {
                            objects: versions
                                .iter()
                                .map(|v| ObjectIdentifier {
                                    key: v.key.as_ref().unwrap().to_owned(),
                                    version_id: v.version_id.to_owned(),
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        },
                        ..Default::default()
                    })
                    .await?;
            }

            if result.is_truncated.is_none() {
                break;
            } else {
                next_key_token = result.next_key_marker;
                next_version_token = result.next_version_id_marker;
            }
        }

        Ok(())
    }

    async fn delete_bucket<'a>(&self, bucket: &str) -> Result<()> {
        debug!(resource = bucket, "Deleting");

        if !self.dry_run {
            if let Ok(()) = self.delete_objects_in_bucket(bucket).await {
                if let Ok(()) = self.delete_versions_in_bucket(bucket).await {
                    self.client
                        .delete_bucket(DeleteBucketRequest {
                            bucket: bucket.to_owned(),
                        })
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn get_tags_for_bucket(&self, bucket: &str) -> Option<Vec<Tag>> {
        let result = self
            .client
            .get_bucket_tagging(GetBucketTaggingRequest {
                bucket: bucket.to_owned(),
            })
            .await;

        if let Ok(tags_output) = result {
            Some(tags_output.tag_set)
        } else {
            None
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
}

#[async_trait]
impl NukerService for S3Service {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Init S3 resource scanner");

        let buckets = self.get_buckets().await?;
        Ok(self.package_buckets_as_resources(buckets).await)
    }

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.delete_bucket(&resource.id).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_bucket(&resource.id).await
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TargetState;
    use regex::Regex;
    use rusoto_mock::{MockCredentialsProvider, MockRequestDispatcher};

    static S3_BUCKET_PREFIX: &str = "^cw-(us-[a-z]*-[0-9]{1})-([0-9]{12})-(.*)$";

    fn create_config() -> S3Config {
        S3Config {
            enabled: true,
            target_state: TargetState::Deleted,
            check_dns_compliant_naming: None,
            check_public_accessibility: None,
            required_naming_prefix: Some(S3_BUCKET_PREFIX.into()),
            required_naming_regex: Some(Regex::new(S3_BUCKET_PREFIX).unwrap()),
            ignore: Vec::new(),
        }
    }

    fn create_s3_client(s3_config: S3Config) -> S3Service {
        S3Service {
            client: S3Client::new_with(
                MockRequestDispatcher::default(),
                MockCredentialsProvider,
                Default::default(),
            ),
            config: s3_config,
            region: Region::UsEast1,
            dry_run: true,
        }
    }

    fn get_s3_buckets() -> Vec<Bucket> {
        vec![
            Bucket {
                creation_date: None,
                name: Some("cw-us-east-1-000000000000-metadata".to_string()),
            },
            Bucket {
                creation_date: None,
                name: Some("cw-us-east-1-000000000000-meta.data".to_string()),
            },
            Bucket {
                creation_date: None,
                name: Some("random-bucket-name".to_string()),
            },
        ]
    }

    fn filter_resources(resources: Vec<Resource>) -> Vec<String> {
        resources
            .into_iter()
            .filter(|r| match r.enforcement_state {
                EnforcementState::Delete => true,
                _ => false,
            })
            .map(|r| r.id)
            .collect()
    }

    #[tokio::test]
    async fn check_package_resources_by_naming() {
        let s3_config = create_config();
        let s3_client = create_s3_client(s3_config);

        let resources = s3_client
            .package_buckets_as_resources(get_s3_buckets())
            .await;
        let result = filter_resources(resources);
        let expected: Vec<String> = vec!["random-bucket-name".to_string()];

        assert_eq!(expected, result)
    }

    #[tokio::test]
    async fn check_package_resources_by_dns_complaint_name() {
        let mut s3_config = create_config();
        s3_config.check_dns_compliant_naming = Some(true);
        let s3_client = create_s3_client(s3_config);

        let resources = s3_client
            .package_buckets_as_resources(get_s3_buckets())
            .await;
        let mut result = filter_resources(resources);
        let mut expected: Vec<String> = vec![
            "random-bucket-name".to_string(),
            "cw-us-east-1-000000000000-meta.data".to_string(),
        ];

        assert_eq!(expected.sort(), result.sort())
    }
}
