use {
    crate::config::IdleRules,
    crate::error::Error as AwsError,
    chrono::{DateTime, TimeZone, Utc},
    log::trace,
    rusoto_cloudwatch::{
        CloudWatch, CloudWatchClient, Dimension, GetMetricStatisticsInput,
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
    pub fn new(profile_name: &String, region: Region, idle_rules: IdleRules) -> Result<Self> {
        let mut pp = ProfileProvider::new()?;
        pp.set_profile(profile_name);

        Ok(CwClient {
            client: CloudWatchClient::new_with(HttpClient::new()?, pp, region),
            idle_rules,
        })
    }

    fn get_instance_metrics(&self, instance_id: &String) -> Result<GetMetricStatisticsOutput> {
        let end_time = Utc::now();
        let idle_rules = self.idle_rules.clone();

        let req = GetMetricStatisticsInput {
            dimensions: Some(vec![Dimension {
                name: "InstanceId".to_owned(),
                value: instance_id.to_owned(),
            }]),
            namespace: "AWS/EC2".to_owned(),
            metric_name: "CPUUtilization".to_owned(),
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

    fn get_db_instance_metrics(&self, instance_name: &String) -> Result<GetMetricStatisticsOutput> {
        let end_time = Utc::now();
        let idle_rules = self.idle_rules.clone();

        let req = GetMetricStatisticsInput {
            dimensions: Some(vec![Dimension {
                name: "DBInstanceIdentifier".to_owned(),
                value: instance_name.to_owned(),
            }]),
            namespace: "AWS/RDS".to_owned(),
            metric_name: "CPUUtilization".to_owned(),
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
            .get_instance_metrics(instance_id)?
            .datapoints
            .unwrap_or_default();

        trace!("Datapoints used for comparison: {:?}", metrics);

        Ok(metrics.iter().any(|metric| {
            metric.maximum.unwrap_or_default() > self.idle_rules.min_utilization as f64
        }))
    }

    pub fn filter_db_instance_by_utilization(&self, instance_name: &String) -> Result<bool> {
        let metrics = self
            .get_db_instance_metrics(instance_name)?
            .datapoints
            .unwrap_or_default();

        trace!("Datapoints used for comparison: {:?}", metrics);

        Ok(metrics.iter().any(|metric| {
            metric.maximum.unwrap_or_default() > self.idle_rules.min_utilization as f64
        }))
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
