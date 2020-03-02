use crate::aws::Result;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use rusoto_sts::{GetCallerIdentityRequest, Sts, StsClient};

pub struct StsNukeClient {
    pub client: StsClient,
    pub region: Region,
}

impl StsNukeClient {
    pub fn new(profile_name: Option<&str>, region: Region) -> Result<Self> {
        if let Some(profile) = profile_name {
            let mut pp = ProfileProvider::new()?;
            pp.set_profile(profile);

            Ok(StsNukeClient {
                client: StsClient::new_with(HttpClient::new()?, pp, region.clone()),
                region,
            })
        } else {
            Ok(StsNukeClient {
                client: StsClient::new(region.clone()),
                region,
            })
        }
    }

    pub fn get_account_number(&self) -> Result<String> {
        Ok(self
            .client
            .get_caller_identity(GetCallerIdentityRequest {})
            .sync()?
            .account
            .unwrap_or_default())
    }
}
