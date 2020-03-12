use crate::{config::IdleRules, error::Error as AwsError, resource::ResourceType};
use chrono::{DateTime, TimeZone, Utc};
use rusoto_cloudwatch::{
    CloudWatch, CloudWatchClient, Datapoint, Dimension, DimensionFilter, GetMetricStatisticsInput,
    GetMetricStatisticsOutput, ListMetricsInput,
};
use std::time::Duration;
use tracing::{trace, warn};

type Result<T, E = AwsError> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct CwClient {
    pub client: CloudWatchClient,
    pub ec2_idle_rules: Option<Vec<IdleRules>>,
    pub ebs_idle_rules: Option<Vec<IdleRules>>,
    pub elb_alb_idle_rules: Option<Vec<IdleRules>>,
    pub elb_nlb_idle_rules: Option<Vec<IdleRules>>,
    pub rds_idle_rules: Option<Vec<IdleRules>>,
    pub aurora_idle_rules: Option<Vec<IdleRules>>,
    pub redshift_idle_rules: Option<Vec<IdleRules>>,
    pub emr_idle_rules: Option<Vec<IdleRules>>,
    pub es_idle_rules: Option<Vec<IdleRules>>,
}

impl CwClient {
    async fn get_metric_statistics_maximum(
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
            .await
            .map_err(|err| AwsError::Internal {
                error: err.to_string(),
            })
    }

    async fn filter_resource(
        &self,
        resource_id: &str,
        dimension: &str,
        resource_type: ResourceType,
    ) -> Result<bool> {
        let mut result = false;
        let idle_rules = match resource_type {
            ResourceType::Ec2Instance | ResourceType::Ec2Address | ResourceType::Ec2Interface => {
                &self.ec2_idle_rules
            }
            ResourceType::EbsVolume | ResourceType::EbsSnapshot => &self.ebs_idle_rules,
            ResourceType::ElbAlb => &self.elb_alb_idle_rules,
            ResourceType::ElbNlb => &self.elb_nlb_idle_rules,
            ResourceType::RDS => &self.rds_idle_rules,
            ResourceType::Aurora => &self.aurora_idle_rules,
            ResourceType::Redshift => &self.redshift_idle_rules,
            ResourceType::EmrCluster => &self.emr_idle_rules,
            ResourceType::EsDomain => &self.es_idle_rules,
            _ => &None,
        };

        if idle_rules.is_some() {
            for idle_rule in idle_rules.as_ref().unwrap() {
                trace!(idle_rule = ?idle_rule, "Checking resource against Idle Rules");

                if self
                    .is_valid_metric(
                        &idle_rule.namespace,
                        dimension,
                        resource_id,
                        &idle_rule.metric,
                    )
                    .await
                {
                    let metrics = self
                        .get_metric_statistics_maximum(
                            dimension.to_string(),
                            resource_id.to_string(),
                            idle_rule.namespace.to_string(),
                            idle_rule.metric.to_string(),
                            idle_rule.duration,
                            idle_rule.granularity,
                        )
                        .await?
                        .datapoints
                        .unwrap_or_default();

                    trace!(
                        resource = resource_id,
                        "Idle Rules DataPoints: {:?}",
                        metrics
                    );
                    result =
                        self.filter_metrics(metrics, idle_rule.minimum.unwrap_or_default() as f64);
                } else {
                    warn!(resource = resource_id, idle_rule = ?idle_rule, "Invalid Metric.");
                    result = true;
                }
            }
        }

        Ok(result)
    }

    pub async fn filter_instance(&self, instance_id: &str) -> Result<bool> {
        self.filter_resource(instance_id, "InstanceId", ResourceType::Ec2Instance)
            .await
    }

    pub async fn filter_volume(&self, volume_id: &str) -> Result<bool> {
        self.filter_resource(volume_id, "VolumeId", ResourceType::EbsVolume)
            .await
    }

    pub async fn filter_db_instance(&self, instance_name: &str) -> Result<bool> {
        self.filter_resource(instance_name, "DBInstanceIdentifier", ResourceType::RDS)
            .await
    }

    pub async fn filter_db_cluster(&self, cluster_identifier: &str) -> Result<bool> {
        self.filter_resource(
            cluster_identifier,
            "DBClusterIdentifier",
            ResourceType::Aurora,
        )
        .await
    }

    pub async fn filter_rs_cluster(&self, cluster_id: &String) -> Result<bool> {
        self.filter_resource(cluster_id, "ClusterIdentifier", ResourceType::Redshift)
            .await
    }

    pub async fn filter_es_domain(&self, domain_name: &String) -> Result<bool> {
        self.filter_resource(domain_name, "DomainName", ResourceType::EsDomain)
            .await
    }

    pub async fn filter_alb_load_balancer(&self, lb_name: &String) -> Result<bool> {
        self.filter_resource(lb_name, "LoadBalancer", ResourceType::ElbAlb)
            .await
    }

    pub async fn filter_nlb_load_balancer(&self, lb_name: &String) -> Result<bool> {
        self.filter_resource(lb_name, "LoadBalancer", ResourceType::ElbNlb)
            .await
    }

    pub async fn filter_emr_cluster(&self, cluster_id: &String) -> Result<bool> {
        // FIXME: make this code generic
        let mut result = false;

        if self.emr_idle_rules.is_some() {
            for idle_rule in self.emr_idle_rules.as_ref().unwrap() {
                let metrics = self
                    .get_metric_statistics_maximum(
                        "JobFlowId".to_string(),
                        cluster_id.to_string(),
                        idle_rule.namespace.to_string(),
                        idle_rule.metric.to_string(),
                        idle_rule.duration,
                        idle_rule.granularity,
                    )
                    .await?
                    .datapoints
                    .unwrap_or_default();

                result = metrics.iter().all(|m| {
                    m.maximum.unwrap_or_default() == idle_rule.minimum.unwrap_or_default() as f64
                });
            }
        }

        Ok(result)
    }

    /// Validates if a given metric is valid for the provided namespace
    async fn is_valid_metric(
        &self,
        namespace: &str,
        dimension_name: &str,
        dimension_val: &str,
        metric_name: &str,
    ) -> bool {
        let result = self
            .client
            .list_metrics(ListMetricsInput {
                dimensions: Some(vec![DimensionFilter {
                    name: dimension_name.into(),
                    value: Some(dimension_val.into()),
                }]),
                metric_name: Some(metric_name.into()),
                namespace: Some(namespace.into()),
                next_token: None,
            })
            .await
            .map_err(|err| AwsError::Internal {
                error: err.to_string(),
            })
            .unwrap()
            .metrics;

        result.is_some() && result.unwrap().len() > 0
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
            ec2_idle_rules: Some(vec![IdleRules {
                namespace: "AWS/EC2".to_string(),
                metric: "CPUUtilization".to_string(),
                minimum: Some(0.0),
                duration: Duration::from_secs(86400),
                granularity: Duration::from_secs(3600),
                connections: Some(100),
            }]),
            ebs_idle_rules: None,
            elb_alb_idle_rules: None,
            elb_nlb_idle_rules: None,
            rds_idle_rules: None,
            aurora_idle_rules: None,
            redshift_idle_rules: None,
            emr_idle_rules: None,
            es_idle_rules: None,
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
        for _ in cw_client.ec2_idle_rules.as_ref().unwrap() {
            assert_eq!(
                cw_client.filter_metrics(get_metrics_for_min_utilization(), 10.0),
                true
            )
        }
    }

    #[test]
    fn check_metrics_filter_by_min_utilization_of_20() {
        let cw_client = &create_client();
        for _ in cw_client.ec2_idle_rules.as_ref().unwrap() {
            assert_eq!(
                cw_client.filter_metrics(get_metrics_for_min_utilization(), 20.0),
                false
            )
        }
    }
}
