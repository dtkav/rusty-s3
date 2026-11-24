use std::time::Duration;

use jiff::Timestamp;
use serde::Deserialize;
use url::Url;

use super::S3Action;
use crate::actions::Method;
use crate::actions::request::{BuildRequest, RequestParts};
use crate::{Bucket, Credentials, Map};

const POLICY_PARAM: &str = "policy";

/// Retrieve a bucket's policy from S3.
///
/// Find out more about `GetBucketPolicy` from the [AWS API Reference][api]
///
/// [api]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy.html
#[derive(Debug, Clone)]
pub struct GetBucketPolicy<'a> {
    bucket: &'a Bucket,
    credentials: Option<&'a Credentials>,

    query: Map<'a>,
    headers: Map<'a>,
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct GetBucketPolicyResponse {
    #[serde(rename = "Version")]
    pub version: String,
    #[serde(rename = "Id")]
    pub id: Option<String>,
}

impl<'a> GetBucketPolicy<'a> {
    #[inline]
    #[must_use]
    pub const fn new(bucket: &'a Bucket, credentials: Option<&'a Credentials>) -> Self {
        Self {
            bucket,
            credentials,

            query: Map::new(),
            headers: Map::new(),
        }
    }

    /// Parse the response from S3.
    ///
    /// # Errors
    ///
    /// If the response cannot be parsed.
    pub fn parse_response(s: &str) -> Result<GetBucketPolicyResponse, serde_json::Error> {
        serde_json::from_str(s)
    }
}

impl<'a> S3Action<'a> for GetBucketPolicy<'a> {
    const METHOD: Method = Method::Get;

    fn query_mut(&mut self) -> &mut Map<'a> {
        &mut self.query
    }

    fn headers_mut(&mut self) -> &mut Map<'a> {
        &mut self.headers
    }

    fn sign_with_time(&self, expires_in: Duration, time: &Timestamp) -> Url {
        let request = self.request_parts();
        match self.credentials {
            Some(credentials) => request.presign(credentials, expires_in, time),
            None => crate::signing::util::add_query_params(request.base_url, self.query.iter()),
        }
    }
}

impl BuildRequest for GetBucketPolicy<'_> {
    fn request_parts(&self) -> RequestParts<'_> {
        RequestParts::new(
            Self::METHOD,
            self.bucket.base_url().clone(),
            self.bucket.region(),
            &self.headers,
            &self.query,
            vec![(POLICY_PARAM, "".into())],
        )
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn aws_example() -> Result<(), serde_json::Error> {
        assert_eq!(
            GetBucketPolicy::parse_response(r#"{"Version":"1"}"#)?,
            GetBucketPolicyResponse {
                version: "1".to_string(),
                id: None
            }
        );

        let content = r#"{
"Version":"2008-10-17",
"Id":"aaaa-bbbb-cccc-dddd",
"Statement" : [
    {
        "Effect":"Deny",
        "Sid":"1",
        "Principal" : {
            "AWS":["111122223333","444455556666"]
        },
        "Action":["s3:*"],
        "Resource":"arn:aws:s3:::bucket/*"
    }
]
}
"#;
        assert_eq!(
            GetBucketPolicy::parse_response(content)?,
            GetBucketPolicyResponse {
                version: "2008-10-17".to_string(),
                id: Some("aaaa-bbbb-cccc-dddd".to_string()),
            }
        );
        Ok(())
    }
}
