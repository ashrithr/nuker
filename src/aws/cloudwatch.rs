use crate::{client::ClientType, config::IdleRules, handle_future_with_return};
use chrono::{DateTime, TimeZone, Utc};
use rusoto_cloudwatch::{
    CloudWatch, CloudWatchClient, Datapoint, Dimension, DimensionFilter, GetMetricStatisticsInput,
    GetMetricStatisticsOutput, ListMetricsInput,
};
use std::time::Duration;
use tracing::{trace, warn};

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
    pub ecs_idle_rules: Option<Vec<IdleRules>>,
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
    ) -> Option<GetMetricStatisticsOutput> {
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

        let req = self.client.get_metric_statistics(req);
        handle_future_with_return!(req).ok()
    }

    async fn filter_resource(
        &self,
        resource_id: &str,
        dimension: &str,
        resource_type: ClientType,
    ) -> bool {
        let mut result = false;
        let idle_rules = match resource_type {
            ClientType::Ec2Instance => &self.ec2_idle_rules,
            ClientType::EbsVolume => &self.ebs_idle_rules,
            ClientType::ElbAlb => &self.elb_alb_idle_rules,
            ClientType::ElbNlb => &self.elb_nlb_idle_rules,
            ClientType::RdsInstance => &self.rds_idle_rules,
            ClientType::RdsCluster => &self.aurora_idle_rules,
            ClientType::RsCluster => &self.redshift_idle_rules,
            ClientType::EmrCluster => &self.emr_idle_rules,
            ClientType::EsDomain => &self.es_idle_rules,
            ClientType::EcsCluster => &self.ecs_idle_rules,
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
                        .await
                        .unwrap_or_default()
                        .datapoints
                        .unwrap_or_default();

                    result = self.filter_metrics(
                        &metrics,
                        idle_rule.minimum.unwrap_or_default() as f64,
                        idle_rule.duration,
                        idle_rule.granularity,
                    );

                    trace!(
                        resource = resource_id,
                        result = result,
                        "Idle Rules DataPoints: {:?}",
                        metrics
                    );
                } else {
                    warn!(resource = resource_id, idle_rule = ?idle_rule, "Invalid Metric.");
                    result = true;
                }
            }
        }

        result
    }

    pub async fn filter_instance(&self, instance_id: &str) -> bool {
        self.filter_resource(instance_id, "InstanceId", ClientType::Ec2Instance)
            .await
    }

    pub async fn filter_volume(&self, volume_id: &str) -> bool {
        self.filter_resource(volume_id, "VolumeId", ClientType::EbsVolume)
            .await
    }

    pub async fn filter_db_instance(&self, instance_name: &str) -> bool {
        self.filter_resource(
            instance_name,
            "DBInstanceIdentifier",
            ClientType::RdsInstance,
        )
        .await
    }

    pub async fn filter_db_cluster(&self, cluster_identifier: &str) -> bool {
        self.filter_resource(
            cluster_identifier,
            "DBClusterIdentifier",
            ClientType::RdsCluster,
        )
        .await
    }

    pub async fn filter_rs_cluster(&self, cluster_id: &str) -> bool {
        self.filter_resource(cluster_id, "ClusterIdentifier", ClientType::RsCluster)
            .await
    }

    pub async fn filter_es_domain(&self, domain_name: &str) -> bool {
        self.filter_resource(domain_name, "DomainName", ClientType::EsDomain)
            .await
    }

    pub async fn filter_alb_load_balancer(&self, lb_name: &str) -> bool {
        self.filter_resource(lb_name, "LoadBalancer", ClientType::ElbAlb)
            .await
    }

    pub async fn filter_nlb_load_balancer(&self, lb_name: &str) -> bool {
        self.filter_resource(lb_name, "LoadBalancer", ClientType::ElbNlb)
            .await
    }

    pub async fn filter_ecs_cluster(&self, cluster_name: &str) -> bool {
        self.filter_resource(cluster_name, "ClusterName", ClientType::EcsCluster)
            .await
    }

    pub async fn filter_emr_cluster(&self, cluster_id: &str) -> bool {
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
                    .await
                    .unwrap_or_default()
                    .datapoints
                    .unwrap_or_default();

                result = metrics.iter().all(|m| {
                    m.maximum.unwrap_or_default() == idle_rule.minimum.unwrap_or_default() as f64
                });
            }
        }

        result
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
            .map_err(|err| err.to_string())
            .unwrap()
            .metrics;

        result.is_some() && result.unwrap().len() > 0
    }

    fn filter_metrics(
        &self,
        metrics: &[Datapoint],
        minimum: f64,
        min_duration: Duration,
        granularity: Duration,
    ) -> bool {
        let min_metrics_count = (min_duration.as_secs() / granularity.as_secs()) as usize;
        if metrics.len() >= min_metrics_count {
            metrics
                .iter()
                .any(|metric| metric.maximum.unwrap_or_default() > minimum)
        } else {
            trace!(
                "# of datapoints retrieved ({}) are less than minimum ({}) to determine if the \
                resource is idle.",
                metrics.len(),
                min_metrics_count
            );
            false
        }
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
            ecs_idle_rules: None,
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
            Datapoint {
                maximum: Some(1.5678123),
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
                cw_client.filter_metrics(
                    &get_metrics_for_min_utilization(),
                    10.0,
                    Duration::from_secs(100),
                    Duration::from_secs(10)
                ),
                true
            )
        }
    }

    #[test]
    fn check_metrics_filter_by_min_num_of_metrics_required() {
        let cw_client = &create_client();
        for _ in cw_client.ec2_idle_rules.as_ref().unwrap() {
            assert_eq!(
                cw_client.filter_metrics(
                    &get_metrics_for_min_utilization(),
                    10.0,
                    Duration::from_secs(1000),
                    Duration::from_secs(10)
                ),
                false
            )
        }
    }

    #[test]
    fn check_metrics_filter_by_min_utilization_of_20() {
        let cw_client = &create_client();
        for _ in cw_client.ec2_idle_rules.as_ref().unwrap() {
            assert_eq!(
                cw_client.filter_metrics(
                    &get_metrics_for_min_utilization(),
                    20.0,
                    Duration::from_secs(100),
                    Duration::from_secs(10)
                ),
                false
            )
        }
    }
}
