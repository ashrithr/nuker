use crate::aws::Result;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use rusoto_sts::{GetCallerIdentityRequest, Sts, StsClient};

#[derive(Clone)]
pub struct StsService {
    pub client: StsClient,
    pub region: Region,
}

impl StsService {
    pub fn new(profile_name: Option<String>, region: Region) -> Result<Self> {
        if let Some(profile) = profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(StsService {
                client: StsClient::new_with(HttpClient::new()?, pp, region.clone()),
                region,
            })
        } else {
            Ok(StsService {
                client: StsClient::new(region.clone()),
                region,
            })
        }
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
