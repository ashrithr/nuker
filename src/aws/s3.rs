use crate::aws::Result;
use crate::config::S3Config;
use crate::service::{NukeService, Resource};
use rusoto_core::HttpClient;
use rusoto_core::Region;
use rusoto_credential::ProfileProvider;
use rusoto_s3::{
    Bucket, Delete, DeleteBucketRequest, DeleteObjectsRequest, ListObjectVersionsRequest,
    ListObjectsV2Request, ObjectIdentifier, S3Client, S3,
};

pub struct S3NukeClient {
    client: S3Client,
    config: S3Config,
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
                dry_run,
            })
        } else {
            Ok(S3NukeClient {
                client: S3Client::new(region.clone()),
                config,
                dry_run,
            })
        }
    }

    fn get_buckets(&self) -> Result<Vec<Bucket>> {
        let result = self.client.list_buckets().sync()?;
        Ok(result.buckets.unwrap_or_default())
    }

    fn filter_by_name_prefix<'a>(&self, buckets: &Vec<&'a Bucket>) -> Vec<&'a Bucket> {
        buckets
            .iter()
            .filter(|b| {
                b.name
                    .as_ref()
                    .unwrap()
                    .starts_with(&self.config.required_naming_prefix)
            })
            .cloned()
            .collect()
    }

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

            std::thread::sleep(std::time::Duration::from_millis(50));
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

            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        Ok(())
    }

    fn delete_bucket<'a>(&self, bucket: &str) -> Result<()> {
        if let Ok(()) = self.delete_objects_in_bucket(bucket) {
            if let Ok(()) = self.delete_versions_in_bucket(bucket) {
                self.client
                    .delete_bucket(DeleteBucketRequest {
                        bucket: bucket.to_owned(),
                    })
                    .sync()?;
            }
        }

        Ok(())
    }
}

impl NukeService for S3NukeClient {
    fn scan(&self) -> Result<Vec<Resource>> {
        unimplemented!()
    }

    fn cleanup(&self, resources: Vec<&Resource>) -> Result<()> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}