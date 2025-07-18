use std::iter;
use std::time::Duration;

use jiff::Timestamp;
use url::Url;

use crate::actions::Method;
use crate::actions::S3Action;
use crate::signing::sign;
use crate::sorting_iter::SortingIterator;
use crate::{Bucket, Credentials, Map};

/// Versioning status for a bucket.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone, Copy)]
pub enum VersioningStatus {
    Enabled,
    Suspended,
}

impl VersioningStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "Enabled",
            Self::Suspended => "Suspended",
        }
    }
}

/// Configure bucket versioning.
///
/// Find out more about `PutBucketVersioning` from the [AWS API Reference][api]
///
/// [api]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html
#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone)]
pub struct PutBucketVersioning<'a> {
    bucket: &'a Bucket,
    credentials: &'a Credentials,
    status: VersioningStatus,
    mfa_delete: Option<bool>,

    query: Map<'a>,
    headers: Map<'a>,
}

impl<'a> PutBucketVersioning<'a> {
    #[must_use]
    pub const fn new(
        bucket: &'a Bucket,
        credentials: &'a Credentials,
        status: VersioningStatus,
    ) -> Self {
        Self {
            bucket,
            credentials,
            status,
            mfa_delete: None,
            query: Map::new(),
            headers: Map::new(),
        }
    }

    /// Sets MFA delete for the bucket configuration.
    pub fn set_mfa_delete(&mut self, enabled: bool) {
        self.mfa_delete = Some(enabled);
    }

    /// Generate the XML body for the request.
    #[must_use]
    pub fn body(&self) -> String {
        let mut body = String::from(
            "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
        );
        body.push_str("<Status>");
        body.push_str(self.status.as_str());
        body.push_str("</Status>");
        if let Some(enabled) = self.mfa_delete {
            body.push_str("<MfaDelete>");
            body.push_str(if enabled { "Enabled" } else { "Disabled" });
            body.push_str("</MfaDelete>");
        }
        body.push_str("</VersioningConfiguration>");
        body
    }
}

impl<'a> S3Action<'a> for PutBucketVersioning<'a> {
    const METHOD: Method = Method::Put;

    fn query_mut(&mut self) -> &mut Map<'a> {
        &mut self.query
    }

    fn headers_mut(&mut self) -> &mut Map<'a> {
        &mut self.headers
    }

    fn sign_with_time(&self, expires_in: Duration, time: &Timestamp) -> Url {
        let url = self.bucket.base_url().clone();
        let query = SortingIterator::new(iter::once(("versioning", "")), self.query.iter());

        sign(
            time,
            Self::METHOD,
            url,
            self.credentials.key(),
            self.credentials.secret(),
            self.credentials.token(),
            self.bucket.region(),
            expires_in.as_secs(),
            query,
            self.headers.iter(),
        )
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::{Bucket, Credentials, UrlStyle};

    #[test]
    fn aws_example() {
        // Fri, 24 May 2013 00:00:00 GMT
        let date = Timestamp::from_second(1369353600).unwrap();
        let expires_in = Duration::from_secs(86400);

        let endpoint = "https://s3.amazonaws.com".parse().unwrap();
        let bucket = Bucket::new(
            endpoint,
            UrlStyle::VirtualHost,
            "examplebucket",
            "us-east-1",
        )
        .unwrap();
        let credentials = Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        );

        let action = PutBucketVersioning::new(&bucket, &credentials, VersioningStatus::Enabled);

        let url = action.sign_with_time(expires_in, &date);
        let expected = "https://examplebucket.s3.amazonaws.com/?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&versioning=&X-Amz-Signature=5507edf05c88e5851c42c3e376155fcad696114350881b32606e76caabefd13f";
        assert_eq!(expected, url.as_str());
    }
}
