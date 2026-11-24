use std::time::Duration;

use jiff::Timestamp;
use url::Url;

use crate::actions::Method;
use crate::actions::S3Action;
use crate::actions::request::{BuildRequest, RequestParts};
use crate::{Bucket, Credentials, Map};

/// Abort multipart upload.
///
/// This also cleans up any previously uploaded part.
///
/// Find out more about `AbortMultipartUpload` from the [AWS API Reference][api]
///
/// [api]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone)]
pub struct AbortMultipartUpload<'a> {
    bucket: &'a Bucket,
    credentials: Option<&'a Credentials>,
    object: &'a str,

    upload_id: &'a str,

    query: Map<'a>,
    headers: Map<'a>,
}

impl<'a> AbortMultipartUpload<'a> {
    #[inline]
    #[must_use]
    pub const fn new(
        bucket: &'a Bucket,
        credentials: Option<&'a Credentials>,
        object: &'a str,
        upload_id: &'a str,
    ) -> Self {
        Self {
            bucket,
            credentials,
            object,

            upload_id,

            query: Map::new(),
            headers: Map::new(),
        }
    }
}

impl<'a> S3Action<'a> for AbortMultipartUpload<'a> {
    const METHOD: Method = Method::Delete;

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
            None => crate::signing::util::add_query_params(
                request.base_url,
                request.query.operation_pairs(),
            ),
        }
    }
}

impl BuildRequest for AbortMultipartUpload<'_> {
    fn request_parts(&self) -> RequestParts<'_> {
        RequestParts::new(
            Self::METHOD,
            self.bucket.object_url(self.object).unwrap(),
            self.bucket.region(),
            &self.headers,
            &self.query,
            vec![("uploadId", self.upload_id.into())],
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

        let action = AbortMultipartUpload::new(&bucket, Some(&credentials), "test.txt", "abcd");

        let url = action.sign_with_time(expires_in, &date);
        let expected = "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&uploadId=abcd&X-Amz-Signature=7670bc768a7cdb5c276a9dddadeefdffb52061f94db6c14b4a9284fdc195bb59";

        assert_eq!(expected, url.as_str());
    }

    #[test]
    fn anonymous_custom_query() {
        let expires_in = Duration::from_secs(86400);

        let endpoint = "https://s3.amazonaws.com".parse().unwrap();
        let bucket = Bucket::new(
            endpoint,
            UrlStyle::VirtualHost,
            "examplebucket",
            "us-east-1",
        )
        .unwrap();

        let action = AbortMultipartUpload::new(&bucket, None, "test.txt", "abcd");
        let url = action.sign(expires_in);
        let expected = "https://examplebucket.s3.amazonaws.com/test.txt?uploadId=abcd";

        assert_eq!(expected, url.as_str());
    }
}
