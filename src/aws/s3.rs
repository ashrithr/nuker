use crate::{
    aws::Result,
    config::S3Config,
    service::{EnforcementState, NTag, NukeService, Resource, ResourceType},
};
use log::{debug, error};
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use rusoto_s3::{
    Bucket, Delete, DeleteBucketRequest, DeleteObjectsRequest, GetBucketAclOutput,
    GetBucketAclRequest, GetBucketTaggingRequest, Grant, ListObjectVersionsRequest,
    ListObjectsV2Request, ObjectIdentifier, S3Client, Tag, S3,
};

pub struct S3NukeClient {
    client: S3Client,
    config: S3Config,
    region: Region,
    dry_run: bool,
}

impl S3NukeClient {
    pub fn new(
        profile_name: Option<&str>,
        region: Region,
        config: S3Config,
        dry_run: bool,
    ) -> Result<Self> {
        if let Some(profile) = profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(S3NukeClient {
                client: S3Client::new_with(HttpClient::new()?, pp, region.clone()),
                config,
                region,
                dry_run,
            })
        } else {
            Ok(S3NukeClient {
                client: S3Client::new(region.clone()),
                config,
                region,
                dry_run,
            })
        }
    }

    fn package_buckets_as_resources(&self, buckets: Vec<Bucket>) -> Vec<Resource> {
        let mut resources: Vec<Resource> = Vec::new();

        for bucket in buckets {
            let bucket_id = bucket.name.unwrap();

            let enforcement_state: EnforcementState = {
                if self.config.ignore.contains(&bucket_id) {
                    EnforcementState::SkipConfig
                } else if self.resource_prefix_does_not_match(&bucket_id) {
                    EnforcementState::Delete
                } else if self.resource_name_is_not_dns_compliant(&bucket_id) {
                    EnforcementState::Delete
                } else {
                    EnforcementState::Skip
                }
            };

            resources.push(Resource {
                id: bucket_id.clone(),
                region: self.region.clone(),
                resource_type: ResourceType::S3Bucket,
                tags: self.package_tags_as_ntags(self.get_tags_for_bucket(&bucket_id)),
                state: Some("Available".to_string()),
                enforcement_state,
            })
        }

        resources
    }

    fn resource_prefix_does_not_match(&self, bucket_id: &str) -> bool {
        !self
            .config
            .required_naming_regex
            .as_ref()
            .unwrap()
            .is_match(bucket_id)
    }

    fn resource_name_is_not_dns_compliant(&self, bucket_id: &str) -> bool {
        if self.config.check_dns_compliant_naming {
            bucket_id.contains('.')
        } else {
            false
        }
    }

    // fn is_bucket_public(&self, bucket_id: &str) -> bool {
    //     let public_permissions = vec!["WRITE", "WRITE_ACP", "READ", "READ_ACP"];
    //
    //     if let Some(grants) = self.get_acls_for_bucket(bucket_id) {
    //         for grant in grants {
    //             if let Some(permission) = grant.permission {
    //                 if public_permissions.contains(permission) {}
    //             }
    //             if let Some(grantee) = grant.grantee {
    //                 if grantee.type_ == "Group".to_string()
    //                     && grantee.uri
    //                         == Some("http://acs.amazonaws.com/groups/global/AllUsers".to_string())
    //                 {
    //                 }
    //             }
    //         }
    //     }
    //
    //     false
    // }

    fn get_buckets(&self) -> Result<Vec<Bucket>> {
        let result = self.client.list_buckets().sync()?;

        Ok(result.buckets.unwrap_or_default())
    }

    // fn get_acls_for_bucket(&self, bucket_id: &str) -> Option<Vec<Grant>> {
    //     match self
    //         .client
    //         .get_bucket_acl(GetBucketAclRequest {
    //             bucket: bucket_id.to_string(),
    //         })
    //         .sync()
    //     {
    //         Ok(result) => result.grants,
    //         Err(err) => {
    //             error!(
    //                 "Failed to get ACL Grants for bucket {} - {:?}",
    //                 bucket_id, err
    //             );
    //
    //             None
    //         }
    //     }
    // }

    fn delete_objects_in_bucket(&self, bucket: &str) -> Result<()> {
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
                .sync()?;

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
                    .sync()?;
            }

            if result.next_continuation_token.is_none() {
                break;
            } else {
                next_token = result.next_continuation_token;
            }
        }

        Ok(())
    }

    fn delete_versions_in_bucket(&self, bucket: &str) -> Result<()> {
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
                .sync()?;

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
                    .sync()?;
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

    fn delete_bucket<'a>(&self, bucket: &str) -> Result<()> {
        debug!("Deleting bucket and its contents: {:?}", bucket);

        if !self.dry_run {
            if let Ok(()) = self.delete_objects_in_bucket(bucket) {
                if let Ok(()) = self.delete_versions_in_bucket(bucket) {
                    self.client
                        .delete_bucket(DeleteBucketRequest {
                            bucket: bucket.to_owned(),
                        })
                        .sync()?;
                }
            }
        }

        Ok(())
    }

    fn get_tags_for_bucket(&self, bucket: &str) -> Option<Vec<Tag>> {
        let result = self
            .client
            .get_bucket_tagging(GetBucketTaggingRequest {
                bucket: bucket.to_owned(),
            })
            .sync();

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

impl NukeService for S3NukeClient {
    fn scan(&self) -> Result<Vec<Resource>> {
        let buckets = self.get_buckets()?;
        Ok(self.package_buckets_as_resources(buckets))
    }

    fn stop(&self, resource: &Resource) -> Result<()> {
        self.delete_bucket(&resource.id)
    }

    fn delete(&self, resource: &Resource) -> Result<()> {
        self.delete_bucket(&resource.id)
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
            check_dns_compliant_naming: false,
            check_public_accessibility: false,
            required_naming_prefix: S3_BUCKET_PREFIX.into(),
            required_naming_regex: Some(Regex::new(S3_BUCKET_PREFIX).unwrap()),
            ignore: Vec::new(),
        }
    }

    fn create_s3_client(s3_config: S3Config) -> S3NukeClient {
        S3NukeClient {
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

    #[test]
    fn check_package_resources_by_naming() {
        let s3_config = create_config();
        let s3_client = create_s3_client(s3_config);

        let resources = s3_client.package_buckets_as_resources(get_s3_buckets());
        let result = filter_resources(resources);
        let expected: Vec<String> = vec!["random-bucket-name".to_string()];

        assert_eq!(expected, result)
    }

    #[test]
    fn check_package_resources_by_dns_complaint_name() {
        let mut s3_config = create_config();
        s3_config.check_dns_compliant_naming = true;
        let s3_client = create_s3_client(s3_config);

        let resources = s3_client.package_buckets_as_resources(get_s3_buckets());
        let mut result = filter_resources(resources);
        let mut expected: Vec<String> = vec![
            "random-bucket-name".to_string(),
            "cw-us-east-1-000000000000-meta.data".to_string(),
        ];

        assert_eq!(expected.sort(), result.sort())
    }
}
