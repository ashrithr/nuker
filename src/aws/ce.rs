use {
    crate::error::Error as AwsError,
    chrono::prelude::*,
    chrono::Duration,
    prettytable::{cell, row, Cell, Row, Table},
    rusoto_ce::{
        CostExplorer, CostExplorerClient, DateInterval, GetCostAndUsageRequest,
        GetCostAndUsageResponse, GroupDefinition, MetricValue, ResultByTime,
    },
    rusoto_core::{HttpClient, Region},
    rusoto_credential::ProfileProvider,
};

type Result<T, E = AwsError> = std::result::Result<T, E>;

pub struct CeClient {
    pub client: CostExplorerClient,
    pub dur: i64,
}

impl CeClient {
    pub fn new(profile_name: &String, dur: i64) -> Result<Self> {
        let mut pp = ProfileProvider::new()?;
        pp.set_profile(profile_name);

        Ok(CeClient {
            client: CostExplorerClient::new_with(HttpClient::new()?, pp, Region::UsEast1),
            dur: dur,
        })
    }

    pub fn get_usage(&self) -> Result<()> {
        let mut next_token: Option<String> = None;
        let mut results: Vec<ResultByTime> = Vec::new();
        let mut table = Table::new();
        let end = Utc::now();
        let start = end - Duration::days(self.dur);

        loop {
            let result: GetCostAndUsageResponse = self
                .client
                .get_cost_and_usage(GetCostAndUsageRequest {
                    granularity: Some("DAILY".to_owned()),
                    time_period: DateInterval {
                        start: start.format("%Y-%m-%d").to_string(),
                        end: end.format("%Y-%m-%d").to_string(),
                    },
                    metrics: Some(vec!["UnblendedCost".to_owned()]),
                    group_by: Some(vec![
                        GroupDefinition {
                            key: Some("LINKED_ACCOUNT".to_owned()),
                            type_: Some("DIMENSION".to_owned()),
                        },
                        GroupDefinition {
                            key: Some("SERVICE".to_owned()),
                            type_: Some("DIMENSION".to_owned()),
                        },
                    ]),
                    next_page_token: next_token,
                    ..Default::default()
                })
                .sync()?;

            if let Some(results_by_time) = result.results_by_time {
                let mut temp_results: Vec<ResultByTime> = results_by_time.into_iter().collect();
                results.append(&mut temp_results);
            }

            if result.next_page_token.is_none() {
                break;
            } else {
                next_token = result.next_page_token;
            }

            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        table.add_row(row![
            "TimePeriod",
            "LinkedAccount",
            "Service",
            "Amount",
            "Unit",
            "Estimated"
        ]);

        for result_by_time in results {
            let start_time = result_by_time.time_period.unwrap().start;
            let estimated = result_by_time.estimated;

            if let Some(groups) = result_by_time.groups {
                for group in groups {
                    let keys = group.keys;

                    if let Some(mut metrics) = group.metrics {
                        let metric_value: MetricValue = metrics.remove("UnblendedCost").unwrap();
                        let amount = metric_value.amount;
                        let unit = metric_value.unit;

                        table.add_row(row!(
                            start_time,
                            keys.as_ref().unwrap()[0],
                            keys.as_ref().unwrap()[1],
                            amount.unwrap(),
                            unit.unwrap(),
                            estimated.unwrap()
                        ));
                    }
                }
            }
        }

        table.printstd();

        Ok(())
    }
}
