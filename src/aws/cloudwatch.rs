use {
    crate::config::IdleRules,
    crate::error::Error as AwsError,
    chrono::{DateTime, TimeZone, Utc},
    log::trace,
    rusoto_cloudwatch::{
        CloudWatch, CloudWatchClient, Datapoint, Dimension, GetMetricStatisticsInput,
        GetMetricStatisticsOutput,
    },
    rusoto_core::{HttpClient, Region},
    rusoto_credential::ProfileProvider,
    std::time::Duration,
};

type Result<T, E = AwsError> = std::result::Result<T, E>;

pub struct CwClient {
    pub client: CloudWatchClient,
    pub idle_rules: IdleRules,
}

impl CwClient {
    pub fn new(profile_name: Option<&str>, region: Region, idle_rules: IdleRules) -> Result<Self> {
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
    ) -> Result<GetMetricStatisticsOutput> {
        let end_time = Utc::now();
        let idle_rules = self.idle_rules.clone();

        let req = GetMetricStatisticsInput {
            dimensions: Some(vec![Dimension {
                name: dimension_name,
                value: dimension_value,
            }]),
            namespace,
            metric_name,
            start_time: self.get_start_time_from_duration(end_time, idle_rules.min_duration),
            end_time: end_time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            period: idle_rules.granularity.as_secs() as i64,
            statistics: Some(vec!["Maximum".to_owned()]),
            ..Default::default()
        };

        trace!("Sending 'get-metric-statistics' request {:?}", req);

        self.client
            .get_metric_statistics(req)
            .sync()
            .map_err(|err| AwsError::CloudWatchError {
                error: err.to_string(),
            })
    }

    pub fn filter_instance_by_utilization(&self, instance_id: &String) -> Result<bool> {
        let metrics = self
            .get_metric_statistics_maximum(
                "InstanceId".to_string(),
                instance_id.to_string(),
                "AWS/EC2".to_string(),
                "CPUUtilization".to_string(),
            )?
            .datapoints
            .unwrap_or_default();

        trace!("Datapoints used for comparison: {:?}", metrics);

        Ok(self.filter_metrics_by_min_utilization(metrics))
    }

    pub fn filter_db_instance_by_utilization(&self, instance_name: &String) -> Result<bool> {
        let metrics = self
            .get_metric_statistics_maximum(
                "DBInstanceIdentifier".to_string(),
                instance_name.to_string(),
                "AWS/RDS".to_string(),
                "CPUUtilization".to_string(),
            )?
            .datapoints
            .unwrap_or_default();

        trace!("Datapoints used for comparison: {:?}", metrics);

        Ok(self.filter_metrics_by_min_utilization(metrics))
    }

    pub fn filter_db_instance_by_connections(&self, instance_name: &String) -> Result<bool> {
        let metrics = self
            .get_metric_statistics_maximum(
                "DBInstanceIdentifier".to_string(),
                instance_name.to_string(),
                "AWS/RDS".to_string(),
                "DatabaseConnections".to_string(),
            )?
            .datapoints
            .unwrap_or_default();

        trace!("Datapoints used for comparison: {:?}", metrics);

        Ok(self.filter_metrics_by_connections(metrics))
    }

    pub fn filter_db_cluster_by_utilization(&self, cluster_identifier: &String) -> Result<bool> {
        let metrics = self
            .get_metric_statistics_maximum(
                "DBClusterIdentifier".to_string(),
                cluster_identifier.to_string(),
                "AWS/RDS".to_string(),
                "CPUUtilization".to_string(),
            )?
            .datapoints
            .unwrap_or_default();

        trace!("Datapoints used for comparison: {:?}", metrics);

        Ok(self.filter_metrics_by_min_utilization(metrics))
    }

    pub fn filter_db_cluster_by_connections(&self, cluster_identifier: &String) -> Result<bool> {
        let metrics = self
            .get_metric_statistics_maximum(
                "DBClusterIdentifier".to_string(),
                cluster_identifier.to_string(),
                "AWS/RDS".to_string(),
                "DatabaseConnections".to_string(),
            )?
            .datapoints
            .unwrap_or_default();

        trace!("Datapoints used for comparison: {:?}", metrics);

        Ok(self.filter_metrics_by_connections(metrics))
    }

    pub fn filter_rs_cluster_by_utilization(&self, cluster_id: &String) -> Result<bool> {
        let metrics = self
            .get_metric_statistics_maximum(
                "ClusterIdentifier".to_string(),
                cluster_id.to_string(),
                "AWS/Redshift".to_string(),
                "CPUUtilization".to_string(),
            )?
            .datapoints
            .unwrap_or_default();

        Ok(self.filter_metrics_by_min_utilization(metrics))
    }

    pub fn filter_rs_cluster_by_connections(&self, cluster_id: &String) -> Result<bool> {
        let metrics = self
            .get_metric_statistics_maximum(
                "ClusterIdentifier".to_string(),
                cluster_id.to_string(),
                "AWS/Redshift".to_string(),
                "DatabaseConnections".to_string(),
            )?
            .datapoints
            .unwrap_or_default();

        trace!("Datapoints used for comparison: {:?}", metrics);

        Ok(self.filter_metrics_by_connections(metrics))
    }

    fn filter_metrics_by_min_utilization(&self, metrics: Vec<Datapoint>) -> bool {
        metrics.iter().any(|metric| {
            metric.maximum.unwrap_or_default() > self.idle_rules.min_utilization as f64
        })
    }

    fn filter_metrics_by_connections(&self, metrics: Vec<Datapoint>) -> bool {
        metrics.iter().any(|metric| {
            metric.maximum.unwrap_or_default()
                > self.idle_rules.connections.unwrap_or_default() as f64
        })
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
            idle_rules: IdleRules {
                enabled: true,
                min_utilization: 0.0,
                min_duration: Duration::from_secs(86400),
                granularity: Duration::from_secs(3600),
                connections: Some(100),
            },
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
        let mut cw_client = create_client();
        cw_client.idle_rules.min_utilization = 10.0;

        let actual = cw_client.filter_metrics_by_min_utilization(get_metrics_for_min_utilization());

        assert_eq!(actual, true)
    }

    #[test]
    fn check_metrics_filter_by_min_utilization_of_20() {
        let mut cw_client = create_client();
        cw_client.idle_rules.min_utilization = 20.0;

        let actual = cw_client.filter_metrics_by_min_utilization(get_metrics_for_min_utilization());

        assert_eq!(actual, false)
    }
}
