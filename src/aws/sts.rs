use crate::Result;
use rusoto_core::{Client, Region};
use rusoto_sts::{GetCallerIdentityRequest, Sts, StsClient};

#[derive(Clone)]
pub struct StsService {
    pub client: StsClient,
}

impl StsService {
    pub fn new(client: &Client, region: &Region) -> Result<Self> {
        Ok(StsService {
            client: StsClient::new_with_client(client.clone(), region.clone()),
        })
    }

    pub async fn get_account_number(&self) -> Result<String> {
        Ok(self
            .client
            .get_caller_identity(GetCallerIdentityRequest {})
            .await?
            .account
            .unwrap_or_default())
    }
}
