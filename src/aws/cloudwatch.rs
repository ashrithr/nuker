use crate::{config::IdleRules, error::Error as AwsError};
use chrono::{DateTime, TimeZone, Utc};
use log::trace;
use rusoto_cloudwatch::{
    CloudWatch, CloudWatchClient, Datapoint, Dimension, GetMetricStatisticsInput,
    GetMetricStatisticsOutput,
};
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use std::time::Duration;

type Result<T, E = AwsError> = std::result::Result<T, E>;

pub struct CwClient {
    pub client: CloudWatchClient,
    pub idle_rules: Vec<IdleRules>,
}

impl CwClient {
    pub fn new(
        profile_name: Option<&str>,
        region: Region,
        idle_rules: Vec<IdleRules>,
    ) -> Result<Self> {
        if let Some(profile) = profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(CwClient {
                client: CloudWatchClient::new_with(HttpClient::new()?, pp, region),
                idle_rules,
            })
        } else {
            Ok(CwClient {
                client: CloudWatchClient::new(region),
                idle_rules,
            })
        }
    }

    fn get_metric_statistics_maximum(
        &self,
        dimension_name: String,
        dimension_value: String,
        namespace: String,
        metric_name: String,
        min_duration: Duration,
        granularity: Duration,
    ) -> Result<GetMetricStatisticsOutput> {
        let end_time = Utc::now();

        let req = GetMetricStatisticsInput {
            dimensions: Some(vec![Dimension {
                name: dimension_name,
                value: dimension_value,
            }]),
            namespace,
            metric_name,
            start_time: self.get_start_time_from_duration(end_time, min_duration),
            end_time: end_time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            period: granularity.as_secs() as i64,
            statistics: Some(vec!["Maximum".to_owned()]),
            ..Default::default()
        };

        self.client
            .get_metric_statistics(req)
            .sync()
            .map_err(|err| AwsError::CloudWatchError {
                error: err.to_string(),
            })
    }

    fn filter_resource(&self, resource_id: &str, dimension: &str) -> Result<bool> {
        let mut result = false;

        for idle_rule in &self.idle_rules {
            let metrics = self
                .get_metric_statistics_maximum(
                    dimension.to_string(),
                    resource_id.to_string(),
                    idle_rule.namespace.to_string(),
                    idle_rule.metric.to_string(),
                    idle_rule.duration,
                    idle_rule.granularity,
                )?
                .datapoints
                .unwrap_or_default();

            trace!("Datapoints used for comparison: {:?}", metrics);
            result = self.filter_metrics(metrics, idle_rule.minimum.unwrap_or_default() as f64);
        }

        Ok(result)
    }

    pub fn filter_instance(&self, instance_id: &str) -> Result<bool> {
        self.filter_resource(instance_id, "InstanceId")
    }

    pub fn filter_volume(&self, volume_id: &str) -> Result<bool> {
        self.filter_resource(volume_id, "VolumeId")
    }

    pub fn filter_db_instance(&self, instance_name: &str) -> Result<bool> {
        self.filter_resource(instance_name, "DBInstanceIdentifier")
    }

    pub fn filter_db_cluster(&self, cluster_identifier: &str) -> Result<bool> {
        self.filter_resource(cluster_identifier, "DBClusterIdentifier")
    }

    pub fn filter_rs_cluster(&self, cluster_id: &String) -> Result<bool> {
        self.filter_resource(cluster_id, "ClusterIdentifier")
    }

    pub fn filter_emr_cluster(&self, cluster_id: &String) -> Result<bool> {
        // FIXME: make this code generic
        let mut result = false;

        for idle_rule in &self.idle_rules {
            let metrics = self
                .get_metric_statistics_maximum(
                    "JobFlowId".to_string(),
                    cluster_id.to_string(),
                    idle_rule.namespace.to_string(),
                    idle_rule.metric.to_string(),
                    idle_rule.duration,
                    idle_rule.granularity,
                )?
                .datapoints
                .unwrap_or_default();

            result = metrics.iter().all(|m| {
                m.maximum.unwrap_or_default() == idle_rule.minimum.unwrap_or_default() as f64
            });
        }

        Ok(result)
    }

    fn filter_metrics(&self, metrics: Vec<Datapoint>, minimum: f64) -> bool {
        metrics
            .iter()
            .any(|metric| metric.maximum.unwrap_or_default() > minimum)
    }

    fn get_start_time_from_duration(
        &self,
        end_time: DateTime<Utc>,
        min_duration: Duration,
    ) -> String {
        let diff = end_time.timestamp_millis() - min_duration.as_millis() as i64;

        format!(
            "{}",
            Utc.timestamp_millis(diff)
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusoto_cloudwatch::Datapoint;
    use rusoto_mock::{MockCredentialsProvider, MockRequestDispatcher};
    use std::time::Duration;

    fn create_client() -> CwClient {
        CwClient {
            client: CloudWatchClient::new_with(
                MockRequestDispatcher::default(),
                MockCredentialsProvider,
                Default::default(),
            ),
            idle_rules: vec![IdleRules {
                namespace: "AWS/EC2".to_string(),
                metric: "CPUUtilization".to_string(),
                minimum: Some(0.0),
                duration: Duration::from_secs(86400),
                granularity: Duration::from_secs(3600),
                connections: Some(100),
            }],
        }
    }

    fn get_metrics_for_min_utilization() -> Vec<Datapoint> {
        vec![
            Datapoint {
                maximum: Some(9.1231234),
                unit: Some("Percent".to_string()),
                ..Default::default()
            },
            Datapoint {
                maximum: Some(10.123123),
                unit: Some("Percent".to_string()),
                ..Default::default()
            },
            Datapoint {
                maximum: Some(5.123123),
                unit: Some("Percent".to_string()),
                ..Default::default()
            },
            Datapoint {
                maximum: Some(1.457890),
                unit: Some("Percent".to_string()),
                ..Default::default()
            },
            Datapoint {
                maximum: Some(11.457890),
                unit: Some("Percent".to_string()),
                ..Default::default()
            },
            Datapoint {
                maximum: Some(4.457890),
                unit: Some("Percent".to_string()),
                ..Default::default()
            },
            Datapoint {
                maximum: Some(2.457890),
                unit: Some("Percent".to_string()),
                ..Default::default()
            },
            Datapoint {
                maximum: Some(0.457890),
                unit: Some("Percent".to_string()),
                ..Default::default()
            },
            Datapoint {
                maximum: Some(0.5647839),
                unit: Some("Percent".to_string()),
                ..Default::default()
            },
        ]
    }

    #[test]
    fn check_metrics_filter_by_min_utilization_of_10() {
        let cw_client = &create_client();
        for _ in &cw_client.idle_rules {
            assert_eq!(
                cw_client.filter_metrics(get_metrics_for_min_utilization(), 10.0),
                true
            )
        }
    }

    #[test]
    fn check_metrics_filter_by_min_utilization_of_20() {
        let cw_client = &create_client();
        for _ in &cw_client.idle_rules {
            assert_eq!(
                cw_client.filter_metrics(get_metrics_for_min_utilization(), 20.0),
                false
            )
        }
    }
}
