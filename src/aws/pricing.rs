use {
    crate::error::Error as AwsError,
    lazy_static::lazy_static,
    log::{error, trace},
    rusoto_core::{HttpClient, Region},
    rusoto_credential::ProfileProvider,
    rusoto_pricing::{
        DescribeServicesRequest, DescribeServicesResponse, Filter, GetProductsRequest,
        GetProductsResponse, Pricing, PricingClient, Service,
    },
    serde_json::{Result as JResult, Value},
    std::collections::HashMap,
};

lazy_static! {
    // Taken from: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html
    static ref REGIONS: HashMap<&'static str, &'static str> = [
        ("us-east-1", "US East (N. Virginia)"),
        ("us-east-2", "US East (Ohio)"),
        ("us-west-1", "US West (N. California)"),
        ("us-west-2", "US West (Oregon)"),
        ("ca-central-1", "Canada (Central)"),
        ("eu-north-1", "EU (Stockholm)"),
        ("eu-west-1", "EU (Ireland)"),
        ("eu-central-1", "EU (Frankfurt)"),
        ("eu-west-2", "EU (London)"),
        ("eu-west-3", "EU (Paris)"),
        ("ap-northeast-1", "Asia Pacific (Tokyo)"),
        ("ap-northeast-2", "Asia Pacific (Seoul)"),
        ("ap-northeast-3", "Asia Pacific (Osaka-Local)"),
        ("ap-southeast-1", "Asia Pacific (Singapore)"),
        ("ap-southeast-2", "Asia Pacific (Sydney)"),
        ("ap-south-1", "Asia Pacific (Mumbai)"),
        ("sa-east-1", "South America (Sao Paulo)"),
        ("us-gov-west-1", "AWS GovCloud (US)"),
        ("us-gov-east-1", "AWS GovCloud (US-East)")
    ]
    .iter()
    .copied()
    .collect();
}

pub enum LEASE_CONTRACT_LENGTH {
    OneYear,
    ThreeYear,
}

impl LEASE_CONTRACT_LENGTH {
    fn value(&self) -> &str {
        match *self {
            LEASE_CONTRACT_LENGTH::OneYear => "1yr",
            LEASE_CONTRACT_LENGTH::ThreeYear => "3yr",
        }
    }
}

pub enum OFFERING_CLASS {
    Standard,
    Convertible,
}

pub enum PURCHASE_OPTION {
    NoUpfront,
    PartialUpfront,
    AllUpfront,
}

impl PURCHASE_OPTION {
    fn value(&self) -> &str {
        match *self {
            PURCHASE_OPTION::NoUpfront => "No Upfront",
            PURCHASE_OPTION::PartialUpfront => "Partial Upfront",
            PURCHASE_OPTION::AllUpfront => "All Upfront",
        }
    }
}

type Result<T, E = AwsError> = std::result::Result<T, E>;

pub struct PriceClient {
    pub client: PricingClient,
}

impl PriceClient {
    pub fn new(profile_name: Option<&str>) -> Result<Self> {
        if let Some(profile) = profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(PriceClient {
                client: PricingClient::new_with(HttpClient::new()?, pp, Region::UsEast1),
            })
        } else {
            Ok(PriceClient {
                client: PricingClient::new(Region::UsEast1),
            })
        }
    }

    fn get_services(&self) -> Result<Vec<Service>> {
        let mut next_token: Option<String> = None;
        let mut services: Vec<Service> = Vec::new();

        loop {
            let result: DescribeServicesResponse = self
                .client
                .describe_services(DescribeServicesRequest {
                    next_token: next_token,
                    ..Default::default()
                })
                .sync()?;

            if let Some(services_rs) = result.services {
                let mut temp_results: Vec<Service> = services_rs.into_iter().collect();
                services.append(&mut temp_results);
            }

            if result.next_token.is_none() {
                break;
            } else {
                next_token = result.next_token;
            }

            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        Ok(services)
    }

    pub fn ec2_on_demand_pricing(
        &self,
        region_short_code: &str,
        instance_type: &str,
    ) -> Result<Option<String>> {
        let mut next_token: Option<String> = None;
        let mut pages: Vec<GetProductsResponse> = Vec::new();
        let mut on_demand_price: Option<String> = None;

        let region = if REGIONS.contains_key(region_short_code) {
            REGIONS.get(region_short_code).unwrap()
        } else {
            REGIONS.get("us-east-1").unwrap()
        };

        loop {
            let result = self
                .client
                .get_products(GetProductsRequest {
                    service_code: Some("AmazonEC2".into()),
                    format_version: Some("aws_v1".into()),
                    filters: Some(vec![
                        Filter {
                            field: "location".to_string(),
                            type_: "TERM_MATCH".to_string(),
                            value: "EU (Ireland)".to_string(),
                        },
                        Filter {
                            field: "instanceType".to_string(),
                            type_: "TERM_MATCH".to_string(),
                            value: "m5.large".to_string(),
                        },
                        Filter {
                            field: "capacitystatus".to_string(),
                            type_: "TERM_MATCH".to_string(),
                            value: "Used".to_string(),
                        },
                        Filter {
                            field: "tenancy".to_string(),
                            type_: "TERM_MATCH".to_string(),
                            value: "Shared".to_string(),
                        },
                        Filter {
                            field: "preInstalledSw".to_string(),
                            type_: "TERM_MATCH".to_string(),
                            value: "NA".to_string(),
                        },
                        Filter {
                            field: "operatingSystem".to_string(),
                            type_: "TERM_MATCH".to_string(),
                            value: "Linux".to_string(),
                        },
                    ]),
                    next_token: next_token,
                    ..Default::default()
                })
                .sync()?;

            if result.next_token.is_none() {
                pages.push(result);
                break;
            } else {
                next_token = result.next_token.clone();
                pages.push(result);
            }

            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        let mut products: Vec<Value> = Vec::new();
        for page in pages {
            for price_item in page.price_list.unwrap() {
                let product_item_json: Value = serde_json::from_str(&price_item)?;
                products.push(product_item_json);
            }
        }

        for product in products {
            if product["terms"]["OnDemand"].is_string() {
                on_demand_price = Some(product["terms"]["OnDemand"].as_str().unwrap().to_string());
                trace!(
                    "On demand price for the instance type: '{}' is: '{:?}'",
                    instance_type,
                    on_demand_price
                );
            }
        }

        Ok(on_demand_price)
    }

    fn fetch_products(&self, offer_name: &String) -> Result<Vec<GetProductsResponse>> {
        let mut next_token: Option<String> = None;
        let mut pages: Vec<GetProductsResponse> = Vec::new();

        loop {
            let result = self
                .client
                .get_products(GetProductsRequest {
                    service_code: Some(offer_name.into()),
                    format_version: Some("aws_v1".into()),
                    next_token: next_token,
                    ..Default::default()
                })
                .sync()?;

            if result.next_token.is_none() {
                break;
            } else {
                next_token = result.next_token.clone();
            }

            pages.push(result);

            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        Ok(pages)
    }

    pub fn fetch_offer(&self, offer_name: &String) -> Result<HashMap<String, Value>> {
        let services: Vec<String> = self.all_services_names()?;
        let mut offer = HashMap::new();

        if services.contains(offer_name) {
            let resp_pages = self.fetch_products(offer_name)?;

            for page in resp_pages {
                for product in page.price_list.unwrap() {
                    let product_offer: Value = serde_json::from_str(&product)?;
                    let sku = product_offer["product"]["sku"]
                        .as_str()
                        .unwrap()
                        .to_string();

                    offer.insert(sku, product_offer);
                }
            }
        } else {
            error!(
                "Unknown offer name, no corresponding AWS service: {}",
                offer_name
            );
        }

        Ok(offer)
    }

    /// Returns all the service names available for pricing queries
    pub fn all_services_names(&self) -> Result<Vec<String>> {
        Ok(self
            .get_services()?
            .into_iter()
            .flat_map(|service| service.service_code)
            .collect())
    }

    pub fn get_ec2_pricing(&self) -> Result<()> {
        unimplemented!()
    }
}
