use crate::{
    client::ClientType, config::FilterOp, config::MetricFilter, config::MetricStatistic,
    handle_future_with_return,
};
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
    pub account_num: String,
    pub ec2_metric_filters: Option<Vec<MetricFilter>>,
    pub ebs_metric_filters: Option<Vec<MetricFilter>>,
    pub elb_alb_metric_filters: Option<Vec<MetricFilter>>,
    pub elb_nlb_metric_filters: Option<Vec<MetricFilter>>,
    pub rds_metric_filters: Option<Vec<MetricFilter>>,
    pub aurora_metric_filters: Option<Vec<MetricFilter>>,
    pub redshift_metric_filters: Option<Vec<MetricFilter>>,
    pub emr_metric_filters: Option<Vec<MetricFilter>>,
    pub es_metric_filters: Option<Vec<MetricFilter>>,
    pub ecs_metric_filters: Option<Vec<MetricFilter>>,
    pub eks_metric_filters: Option<Vec<MetricFilter>>,
}

impl CwClient {
    async fn get_metric_statistics_maximum(
        &self,
        dimensions: Vec<Dimension>,
        namespace: String,
        metric_name: String,
        duration: Duration,
        granularity: Duration,
        statistic: MetricStatistic,
    ) -> Option<GetMetricStatisticsOutput> {
        let end_time = Utc::now();

        let req = GetMetricStatisticsInput {
            dimensions: Some(dimensions),
            namespace,
            metric_name,
            start_time: self.get_start_time_from_duration(end_time, duration),
            end_time: end_time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            period: granularity.as_secs() as i64,
            statistics: Some(vec![statistic.to_string()]),
            ..Default::default()
        };

        let req = self.client.get_metric_statistics(req);
        handle_future_with_return!(req).ok()
    }

    async fn filter_resource(
        &self,
        resource_id: &str,
        default_dimensions: Vec<Dimension>,
        resource_type: ClientType,
    ) -> bool {
        let mut result = false;
        let metric_filters = match resource_type {
            ClientType::Ec2Instance => &self.ec2_metric_filters,
            ClientType::EbsVolume => &self.ebs_metric_filters,
            ClientType::ElbAlb => &self.elb_alb_metric_filters,
            ClientType::ElbNlb => &self.elb_nlb_metric_filters,
            ClientType::RdsInstance => &self.rds_metric_filters,
            ClientType::RdsCluster => &self.aurora_metric_filters,
            ClientType::RsCluster => &self.redshift_metric_filters,
            ClientType::EmrCluster => &self.emr_metric_filters,
            ClientType::EsDomain => &self.es_metric_filters,
            ClientType::EcsCluster => &self.ecs_metric_filters,
            ClientType::EksCluster => &self.eks_metric_filters,
            _ => &None,
        };
        let namespace = match resource_type {
            ClientType::Ec2Instance => Some("AWS/EC2"),
            ClientType::EbsVolume => Some("AWS/EBS"),
            ClientType::ElbAlb => Some("AWS/ApplicationELB"),
            ClientType::ElbNlb => Some("AWS/NetworkELB"),
            ClientType::RdsInstance => Some("AWS/RDS"),
            ClientType::RdsCluster => Some("AWS/RDS"),
            ClientType::RsCluster => Some("AWS/Redshift"),
            ClientType::EmrCluster => Some("AWS/ElasticMapReduce"),
            ClientType::EsDomain => Some("AWS/ES"),
            ClientType::EcsCluster => Some("AWS/ECS"),
            ClientType::EksCluster => Some("AWS/EKS"),
            _ => None,
        };

        if metric_filters.is_some() && namespace.is_some() {
            for metric_filter in metric_filters.as_ref().unwrap() {
                trace!(metric_filter = ?metric_filter, "Checking resource against Idle Rules");

                let mut dimensions: Vec<Dimension> = Vec::new();
                let user_dimensions: Vec<Dimension> = metric_filter
                    .dimensions
                    .as_deref()
                    .unwrap_or(&Vec::new())
                    .iter()
                    .map(|d| Dimension {
                        name: d.name.to_string(),
                        value: d.value.to_string(),
                    })
                    .collect();
                dimensions.extend(default_dimensions.clone());
                dimensions.extend(user_dimensions);

                if self
                    .is_valid_metric(
                        namespace.unwrap(),
                        &self.to_dimension_filters(&dimensions[..])[..],
                        &metric_filter.name,
                    )
                    .await
                {
                    let metrics = self
                        .get_metric_statistics_maximum(
                            dimensions,
                            namespace.unwrap().to_string(),
                            metric_filter.name.to_string(),
                            metric_filter.duration,
                            metric_filter.period,
                            metric_filter.statistic,
                        )
                        .await
                        .unwrap_or_default()
                        .datapoints
                        .unwrap_or_default();

                    result = self.filter_metrics(
                        &metrics,
                        metric_filter.value as f64,
                        metric_filter.duration,
                        metric_filter.period,
                        metric_filter.op,
                        metric_filter.statistic,
                    );

                    trace!(
                        resource = resource_id,
                        result = result,
                        "Idle Rules DataPoints: {:?}",
                        metrics
                    );
                } else {
                    warn!(resource = resource_id, metric_filter = ?metric_filter, "Invalid Metric.");
                    result = false;
                }
            }
        }

        result
    }

    pub async fn filter_instance(&self, instance_id: &str) -> bool {
        self.filter_resource(
            instance_id,
            vec![Dimension {
                name: "InstanceId".to_string(),
                value: instance_id.to_string(),
            }],
            ClientType::Ec2Instance,
        )
        .await
    }

    pub async fn filter_volume(&self, volume_id: &str) -> bool {
        self.filter_resource(
            volume_id,
            vec![Dimension {
                name: "VolumeId".to_string(),
                value: volume_id.to_string(),
            }],
            ClientType::EbsVolume,
        )
        .await
    }

    pub async fn filter_db_instance(&self, instance_name: &str) -> bool {
        self.filter_resource(
            instance_name,
            vec![Dimension {
                name: "DBInstanceIdentifier".to_string(),
                value: instance_name.to_string(),
            }],
            ClientType::RdsInstance,
        )
        .await
    }

    pub async fn filter_db_cluster(&self, cluster_identifier: &str) -> bool {
        self.filter_resource(
            cluster_identifier,
            vec![Dimension {
                name: "DBClusterIdentifier".to_string(),
                value: cluster_identifier.to_string(),
            }],
            ClientType::RdsCluster,
        )
        .await
    }

    pub async fn filter_rs_cluster(&self, cluster_id: &str) -> bool {
        self.filter_resource(
            cluster_id,
            vec![Dimension {
                name: "ClusterIdentifier".to_string(),
                value: cluster_id.to_string(),
            }],
            ClientType::RsCluster,
        )
        .await
    }

    pub async fn filter_es_domain(&self, domain_name: &str) -> bool {
        self.filter_resource(
            domain_name,
            vec![
                Dimension {
                    name: "DomainName".to_string(),
                    value: domain_name.to_string(),
                },
                Dimension {
                    name: "ClientId".to_string(),
                    value: self.account_num.to_string(),
                },
            ],
            ClientType::EsDomain,
        )
        .await
    }

    pub async fn filter_alb_load_balancer(&self, lb_name: &str) -> bool {
        self.filter_resource(
            lb_name,
            vec![Dimension {
                name: "LoadBalancer".to_string(),
                value: lb_name.to_string(),
            }],
            ClientType::ElbAlb,
        )
        .await
    }

    pub async fn filter_nlb_load_balancer(&self, lb_name: &str) -> bool {
        self.filter_resource(
            lb_name,
            vec![Dimension {
                name: "LoadBalancer".to_string(),
                value: lb_name.to_string(),
            }],
            ClientType::ElbNlb,
        )
        .await
    }

    pub async fn filter_ecs_cluster(&self, cluster_name: &str) -> bool {
        self.filter_resource(
            cluster_name,
            vec![Dimension {
                name: "ClusterName".to_string(),
                value: cluster_name.to_string(),
            }],
            ClientType::EcsCluster,
        )
        .await
    }

    pub async fn filter_eks_cluster(&self, cluster_name: &str) -> bool {
        self.filter_resource(
            cluster_name,
            vec![Dimension {
                name: "ClusterName".to_string(),
                value: cluster_name.to_string(),
            }],
            ClientType::EksCluster,
        )
        .await
    }

    pub async fn filter_emr_cluster(&self, cluster_id: &str) -> bool {
        // FIXME: make this code generic
        // let mut result = false;

        // if self.emr_metric_filters.is_some() {
        //     for metric_filter in self.emr_metric_filters.as_ref().unwrap() {
        //         let metrics = self
        //             .get_metric_statistics_maximum(
        //                 vec![Dimension {
        //                     name: "JobFlowId".to_string(),
        //                     value: cluster_id.to_string(),
        //                 }],
        //                 metric_filter.namespace.to_string(),
        //                 metric_filter.metric.to_string(),
        //                 metric_filter.duration,
        //                 metric_filter.granularity,
        //             )
        //             .await
        //             .unwrap_or_default()
        //             .datapoints
        //             .unwrap_or_default();

        //         result = metrics.iter().all(|m| {
        //             m.maximum.unwrap_or_default()
        //                 == metric_filter.minimum.unwrap_or_default() as f64
        //         });
        //     }
        // }

        // result
        self.filter_resource(
            cluster_id,
            vec![Dimension {
                name: "JobFlowId".to_string(),
                value: cluster_id.to_string(),
            }],
            ClientType::EmrCluster,
        )
        .await
    }

    /// Validates if a given metric is valid for the provided namespace
    async fn is_valid_metric(
        &self,
        namespace: &str,
        dimension_filters: &[DimensionFilter],
        metric_name: &str,
    ) -> bool {
        let result = self
            .client
            .list_metrics(ListMetricsInput {
                dimensions: Some(dimension_filters.to_vec()),
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
        value: f64,
        min_duration: Duration,
        granularity: Duration,
        op: FilterOp,
        statistic: MetricStatistic,
    ) -> bool {
        let min_metrics_count = (min_duration.as_secs() / granularity.as_secs()) as usize;
        if metrics.len() >= min_metrics_count {
            self.compare_metrics(metrics, value, op, statistic)
        } else {
            trace!(
                "# of data-points retrieved ({}) are less than minimum ({}) to determine if the \
                resource is idle.",
                metrics.len(),
                min_metrics_count
            );
            false
        }
    }

    fn compare_metrics(
        &self,
        metrics: &[Datapoint],
        value: f64,
        op: FilterOp,
        statistic: MetricStatistic,
    ) -> bool {
        match op {
            FilterOp::Lt => match statistic {
                MetricStatistic::SampleCount => metrics
                    .iter()
                    .all(|metric| metric.sample_count.unwrap_or_default() < value),
                MetricStatistic::Average => metrics
                    .iter()
                    .all(|metric| metric.average.unwrap_or_default() < value),
                MetricStatistic::Sum => metrics
                    .iter()
                    .all(|metric| metric.sum.unwrap_or_default() < value),
                MetricStatistic::Minimum => metrics
                    .iter()
                    .all(|metric| metric.minimum.unwrap_or_default() < value),
                MetricStatistic::Maximum => metrics
                    .iter()
                    .all(|metric| metric.maximum.unwrap_or_default() < value),
            },
            FilterOp::Gt => match statistic {
                MetricStatistic::SampleCount => metrics
                    .iter()
                    .all(|metric| metric.sample_count.unwrap_or_default() > value),
                MetricStatistic::Average => metrics
                    .iter()
                    .all(|metric| metric.average.unwrap_or_default() > value),
                MetricStatistic::Sum => metrics
                    .iter()
                    .all(|metric| metric.sum.unwrap_or_default() > value),
                MetricStatistic::Minimum => metrics
                    .iter()
                    .all(|metric| metric.minimum.unwrap_or_default() > value),
                MetricStatistic::Maximum => metrics
                    .iter()
                    .all(|metric| metric.maximum.unwrap_or_default() > value),
            },
            FilterOp::Le => match statistic {
                MetricStatistic::SampleCount => metrics
                    .iter()
                    .all(|metric| metric.sample_count.unwrap_or_default() <= value),
                MetricStatistic::Average => metrics
                    .iter()
                    .all(|metric| metric.average.unwrap_or_default() <= value),
                MetricStatistic::Sum => metrics
                    .iter()
                    .all(|metric| metric.sum.unwrap_or_default() <= value),
                MetricStatistic::Minimum => metrics
                    .iter()
                    .all(|metric| metric.minimum.unwrap_or_default() <= value),
                MetricStatistic::Maximum => metrics
                    .iter()
                    .all(|metric| metric.maximum.unwrap_or_default() <= value),
            },
            FilterOp::Ge => match statistic {
                MetricStatistic::SampleCount => metrics
                    .iter()
                    .all(|metric| metric.sample_count.unwrap_or_default() >= value),
                MetricStatistic::Average => metrics
                    .iter()
                    .all(|metric| metric.average.unwrap_or_default() >= value),
                MetricStatistic::Sum => metrics
                    .iter()
                    .all(|metric| metric.sum.unwrap_or_default() >= value),
                MetricStatistic::Minimum => metrics
                    .iter()
                    .all(|metric| metric.minimum.unwrap_or_default() >= value),
                MetricStatistic::Maximum => metrics
                    .iter()
                    .all(|metric| metric.maximum.unwrap_or_default() >= value),
            },
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

    fn to_dimension_filters(&self, dimensions: &[Dimension]) -> Vec<DimensionFilter> {
        let mut df = Vec::new();

        for d in dimensions {
            df.push(DimensionFilter {
                name: d.name.to_string(),
                value: Some(d.value.to_string()),
            })
        }

        df
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
            account_num: "1234567890".to_string(),
            ec2_metric_filters: Some(vec![MetricFilter {
                name: "CPUUtilization".to_string(),
                value: 0.0,
                duration: Duration::from_secs(86400),
                period: Duration::from_secs(3600),
                statistic: MetricStatistic::Maximum,
                ..Default::default()
            }]),
            ebs_metric_filters: None,
            elb_alb_metric_filters: None,
            elb_nlb_metric_filters: None,
            rds_metric_filters: None,
            aurora_metric_filters: None,
            redshift_metric_filters: None,
            emr_metric_filters: None,
            es_metric_filters: None,
            ecs_metric_filters: None,
            eks_metric_filters: None,
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
        for _ in cw_client.ec2_metric_filters.as_ref().unwrap() {
            assert_eq!(
                cw_client.filter_metrics(
                    &get_metrics_for_min_utilization(),
                    12.0,
                    Duration::from_secs(100),
                    Duration::from_secs(10),
                    FilterOp::Lt,
                    MetricStatistic::Maximum
                ),
                true
            )
        }
    }

    #[test]
    fn check_metrics_filter_by_min_num_of_metrics_required() {
        let cw_client = &create_client();
        for _ in cw_client.ec2_metric_filters.as_ref().unwrap() {
            assert_eq!(
                cw_client.filter_metrics(
                    &get_metrics_for_min_utilization(),
                    10.0,
                    Duration::from_secs(1000),
                    Duration::from_secs(10),
                    FilterOp::Lt,
                    MetricStatistic::Maximum
                ),
                false
            )
        }
    }

    #[test]
    fn check_metrics_filter_by_min_utilization_of_20() {
        let cw_client = &create_client();
        for _ in cw_client.ec2_metric_filters.as_ref().unwrap() {
            assert_eq!(
                cw_client.filter_metrics(
                    &get_metrics_for_min_utilization(),
                    10.0,
                    Duration::from_secs(100),
                    Duration::from_secs(10),
                    FilterOp::Lt,
                    MetricStatistic::Maximum
                ),
                false
            )
        }
    }
}
