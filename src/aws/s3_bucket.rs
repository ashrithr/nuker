use crate::aws::ClientDetails;
use crate::client::{ClientType, NukerClient};
use crate::config::ResourceConfig;
use crate::handle_future_with_return;
use crate::resource::{EnforcementState, NTag, Resource, ResourceState};
use crate::{Error, Result};
use async_trait::async_trait;
use rusoto_core::Region;
use rusoto_s3::{
    Bucket, Delete, DeleteBucketPolicyRequest, DeleteBucketRequest, DeleteObjectsRequest,
    GetBucketAclRequest, GetBucketLocationRequest, GetBucketPolicyRequest,
    GetBucketPolicyStatusRequest, GetBucketTaggingRequest, GetBucketVersioningRequest,
    GetPublicAccessBlockRequest, Grant, ListObjectVersionsRequest, ListObjectsV2Request,
    ObjectIdentifier, PolicyStatus, PublicAccessBlockConfiguration, PutBucketVersioningRequest,
    S3Client, Tag, VersioningConfiguration, S3,
};
use tracing::{debug, trace, warn};

static S3_PUBLIC_GROUPS: [&str; 2] = [
    "http://acs.amazonaws.com/groups/global/AuthenticatedUsers",
    "http://acs.amazonaws.com/groups/global/AllUsers",
];

#[derive(Clone)]
pub struct S3BucketClient {
    client: S3Client,
    region: Region,
    account_num: String,
    config: ResourceConfig,
    dry_run: bool,
}

impl S3BucketClient {
    pub fn new(cd: &ClientDetails, config: &ResourceConfig, dry_run: bool) -> Self {
        S3BucketClient {
            client: S3Client::new_with_client(cd.client.clone(), cd.region.clone()),
            region: cd.region.clone(),
            account_num: cd.account_number.clone(),
            config: config.clone(),
            dry_run,
        }
    }

    async fn package_resources(&self, buckets: Vec<Bucket>) -> Vec<Resource> {
        let mut resources: Vec<Resource> = Vec::new();

        for bucket in buckets {
            let bucket_name = bucket.name.unwrap();
            let arn = format!("arn:aws:s3:::{}", bucket_name);
            let tags = self.get_tags_for_bucket(&bucket_name).await;

            resources.push(Resource {
                id: bucket_name,
                arn: Some(arn),
                type_: ClientType::S3Bucket,
                region: self.region.clone(),
                tags: self.package_tags(tags),
                state: Some(ResourceState::Available),
                start_time: None,
                enforcement_state: EnforcementState::SkipUnknownState,
                resource_type: None,
                dependencies: None,
                termination_protection: None,
            })
        }

        resources
    }

    fn resource_name_is_not_dns_compliant(&self, bucket_id: &str) -> bool {
        bucket_id.contains('.')
    }

    async fn is_bucket_public(&self, bucket_id: &str) -> bool {
        let mut ps_is_public = false;
        let mut grants_is_public = false;

        // Check the Bucket Policy Status
        if let Some(policy_status) = self.get_bucket_policy_status(bucket_id).await {
            if policy_status.is_public == Some(true) {
                debug!("Bucket ({}) policy status is public", bucket_id);
                ps_is_public = true;
            }
        }

        // Check the ACLs to see if grantee is AllUsers or AllAuthenticatedUsers
        if let Some(grants) = self.get_acls_for_bucket(bucket_id).await {
            debug!("Bucket ({}) grants - {:?}", bucket_id, grants);
            for grant in grants {
                if let Some(grantee) = grant.grantee {
                    // TODO: https://github.com/rusoto/rusoto/issues/1703 - type_ is not getting populated
                    // if grantee.type_.as_str() == "Group" &&
                    if let Some(group_type) = grantee.uri {
                        if S3_PUBLIC_GROUPS.contains(&group_type.as_str()) {
                            debug!("Bucket ({}) grants are public", bucket_id);
                            grants_is_public = true;
                        }
                    }
                }
            }
        }

        if ps_is_public || grants_is_public {
            true
        } else {
            false
        }
    }

    async fn get_buckets(&self) -> Result<Vec<Bucket>> {
        let result = self.client.list_buckets().await?;
        let mut buckets: Vec<Bucket> = Vec::new();

        for bucket in result.buckets.unwrap_or_default() {
            match self
                .client
                .get_bucket_location(GetBucketLocationRequest {
                    bucket: bucket.name.as_ref().unwrap().to_string(),
                })
                .await
            {
                Ok(bucket_loc_res) => {
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
                Err(err) => {
                    warn!(
                        resource = ?bucket.name,
                        reason = ?err,
                        "Failed getting bucket location."
                    );
                }
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
        trace!(resource = bucket, "Deleting objects");
        let mut next_token: Option<String> = None;

        // Delete all objects from the bucket
        loop {
            let req = self.client.list_objects_v2(ListObjectsV2Request {
                bucket: bucket.to_owned(),
                continuation_token: next_token,
                ..Default::default()
            });

            if let Ok(result) = handle_future_with_return!(req) {
                if let Some(objects) = result.contents {
                    debug!(
                        resource = bucket,
                        objects_count = objects.len(),
                        "Deleting Objects"
                    );

                    match self
                        .client
                        .delete_objects(DeleteObjectsRequest {
                            bucket: bucket.to_owned(),
                            // bypass_governance_retention: Some(true),
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
                        .await
                    {
                        Ok(r) => {
                            if let Some(errors) = r.errors {
                                for error in errors {
                                    warn!(
                                        resource = bucket,
                                        "Failed delete_object with errors: {:?}", error
                                    );
                                }
                            } else {
                                debug!(
                                    resource = bucket,
                                    deleted_count = r.deleted.unwrap().len(),
                                    "Successfully deleted objects"
                                );
                            }
                        }
                        Err(err) => {
                            warn!(resource = bucket, error = ?err, "Failed delete_objects");
                        }
                    }
                }

                if result.next_continuation_token.is_none() {
                    break;
                } else {
                    next_token = result.next_continuation_token;
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    async fn delete_versions_in_bucket(&self, bucket: &str) -> Result<()> {
        trace!(resource = bucket, "Deleting Object Versions");

        // Delete all object versions(required for versioned buckets)
        let mut next_key_token: Option<String> = None;
        loop {
            match self
                .client
                .list_object_versions(ListObjectVersionsRequest {
                    bucket: bucket.to_owned(),
                    key_marker: next_key_token,
                    ..Default::default()
                })
                .await
            {
                Ok(result) => {
                    trace!(
                        resource = bucket,
                        is_truncated = ?result.is_truncated,
                        "list_object_versions"
                    );

                    if let Some(versions) = result.versions {
                        debug!(
                            resource = bucket,
                            versions_count = versions.len(),
                            "Deleting Object Versions"
                        );

                        // delete object versions
                        match self
                            .client
                            .delete_objects(DeleteObjectsRequest {
                                bucket: bucket.to_owned(),
                                // bypass_governance_retention: Some(true),
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
                            .await
                        {
                            Ok(r) => {
                                if let Some(errors) = r.errors {
                                    for error in errors {
                                        warn!(
                                            resource = bucket,
                                            "Failed delete_object with errors: {:?}", error
                                        );
                                    }
                                } else {
                                    debug!(
                                        resource = bucket,
                                        deleted_count = r.deleted.unwrap().len(),
                                        "Deleted objects"
                                    );
                                }
                            }
                            Err(err) => {
                                warn!(resource = bucket, error = ?err, "Failed delete_objects - versions");
                            }
                        }
                    }

                    if let Some(delete_markers) = result.delete_markers {
                        debug!(
                            resource = bucket,
                            del_markers_count = delete_markers.len(),
                            "Deleting Object Delete Markers"
                        );

                        // delete object versions
                        match self
                            .client
                            .delete_objects(DeleteObjectsRequest {
                                bucket: bucket.to_owned(),
                                // bypass_governance_retention: Some(true),
                                delete: Delete {
                                    objects: delete_markers
                                        .iter()
                                        .map(|m| ObjectIdentifier {
                                            key: m.key.as_ref().unwrap().to_owned(),
                                            version_id: m.version_id.to_owned(),
                                            ..Default::default()
                                        })
                                        .collect(),
                                    ..Default::default()
                                },
                                ..Default::default()
                            })
                            .await
                        {
                            Ok(r) => {
                                if let Some(errors) = r.errors {
                                    for error in errors {
                                        warn!(
                                            resource = bucket,
                                            "Failed delete_object_markers with errors: {:?}", error
                                        );
                                    }
                                } else {
                                    debug!(
                                        resource = bucket,
                                        deleted_count = r.deleted.unwrap().len(),
                                        "Deleted objects markers"
                                    );
                                }
                            }
                            Err(err) => {
                                warn!(resource = bucket, error = ?err, "Failed delete_objects - delete_markers");
                            }
                        }
                    }

                    if result.is_truncated == Some(true) {
                        next_key_token = result.next_key_marker;
                    } else {
                        break;
                    }
                }
                Err(err) => {
                    warn!(resource = bucket, error = ?err, "Failed getting object versions.");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn suspend_versioning(&self, bucket: &str) -> Result<()> {
        debug!(resource = bucket, "Checking and suspending versioning");

        match self
            .client
            .get_bucket_versioning(GetBucketVersioningRequest {
                bucket: bucket.to_owned(),
            })
            .await
        {
            Ok(result) => {
                if let Some(status) = result.status {
                    match &*status {
                        "Enabled" => {
                            match self
                                .client
                                .put_bucket_versioning(PutBucketVersioningRequest {
                                    bucket: bucket.to_owned(),
                                    versioning_configuration: VersioningConfiguration {
                                        status: Some("Suspended".to_string()),
                                        ..Default::default()
                                    },
                                    ..Default::default()
                                })
                                .await
                            {
                                Ok(()) => {
                                    debug!(resource = bucket, "Suspended versioning.");
                                }
                                Err(err) => {
                                    warn!(resource = bucket, error = ?err, "Failed disabling versioning");
                                    return Err(Error::from(err));
                                }
                            }
                        }
                        "Suspended" => {
                            trace!(resource = bucket, "Versioning is disabled.");
                        }
                        _ => {}
                    }
                }
            }
            Err(err) => {
                warn!(resource = bucket, error = ?err, "Failed  versioning status.");
            }
        }

        Ok(())
    }

    async fn delete_bucket_policy(&self, bucket: &str) -> Result<()> {
        debug!(resource = bucket, "Checking and deleting bucket policy");

        match self
            .client
            .get_bucket_policy(GetBucketPolicyRequest {
                bucket: bucket.to_string(),
            })
            .await
        {
            Ok(result) => {
                if let Some(_policy) = result.policy {
                    match self
                        .client
                        .delete_bucket_policy(DeleteBucketPolicyRequest {
                            bucket: bucket.to_string(),
                        })
                        .await
                    {
                        Ok(()) => trace!(resource = bucket, "Deleted bucket policy"),
                        Err(err) => {
                            warn!(resource = bucket, error = ?err, "Failed deleting bucket policy")
                        }
                    }
                } else {
                    trace!(resource = bucket, "No policy associated with bucket.");
                }
            }
            Err(err) => warn!(resource = bucket, error = ?err, "Failed fetching bucket policy."),
        }

        Ok(())
    }

    async fn delete_bucket<'a>(&self, bucket: &str) -> Result<()> {
        debug!(resource = bucket, "Deleting");

        if !self.dry_run {
            if let Ok(()) = self.delete_bucket_policy(bucket).await {
                if let Ok(()) = self.suspend_versioning(bucket).await {
                    if let Ok(()) = self.delete_objects_in_bucket(bucket).await {
                        if let Ok(()) = self.delete_versions_in_bucket(bucket).await {
                            trace!(resource = bucket, "Attempting to delete bucket");

                            match self
                                .client
                                .delete_bucket(DeleteBucketRequest {
                                    bucket: bucket.to_owned(),
                                })
                                .await
                            {
                                Ok(()) => trace!(resource = bucket, "Successfully deleted bucket."),
                                Err(err) => {
                                    warn!(resource = bucket, error = ?err, "Failed deleting bucket.")
                                }
                            }
                        }
                    }
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

    fn package_tags(&self, tags: Option<Vec<Tag>>) -> Option<Vec<NTag>> {
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
impl NukerClient for S3BucketClient {
    async fn scan(&self) -> Result<Vec<Resource>> {
        trace!("Init S3 Bucket resource scanner");

        let buckets = self.get_buckets().await?;
        Ok(self.package_resources(buckets).await)
    }

    async fn dependencies(&self, _resource: &Resource) -> Option<Vec<Resource>> {
        None
    }

    async fn additional_filters(
        &self,
        resource: &Resource,
        _config: &ResourceConfig,
    ) -> Option<bool> {
        if self.resource_name_is_not_dns_compliant(&resource.id) {
            debug!(
                resource = resource.id.as_str(),
                "Bucket naming is not DNS compliant."
            );
            return Some(true);
        } else if self.is_bucket_public(&resource.id).await {
            debug!(
                resource = resource.id.as_str(),
                "Bucket is publicly accessible."
            );
            return Some(true);
        }

        Some(false)
    }

    async fn stop(&self, resource: &Resource) -> Result<()> {
        self.delete_bucket(&resource.id).await
    }

    async fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_bucket(&resource.id).await
    }
}
