use std::borrow::Cow;
use std::time::Duration;

use jiff::Timestamp;
use url::Url;

use crate::signing::{canonical_query_string, sign};
use crate::sorting_iter::SortingIterator;
use crate::{Credentials, Map, Method};

/// Build an S3 request for an external signer.
///
/// The signer checks the caller's access and signs the URL with `UNSIGNED-PAYLOAD`.
/// Send the body directly to S3 and include the header values used for signing.
///
/// ```
/// use rusty_s3::{Bucket, Method, PrepareRequest, S3Action, UrlStyle};
///
/// let endpoint = "https://s3.us-west-2.amazonaws.com".parse()?;
/// let bucket = Bucket::new(endpoint, UrlStyle::Path, "backups", "us-west-2")?;
/// let mut put = bucket.put_object(None, "docs/notes.txt");
/// put.headers_mut().insert("if-none-match", "*");
///
/// let request = put.unsigned_request();
/// assert_eq!(request.method, Method::Put);
/// assert_eq!(request.region, "us-west-2");
/// assert_eq!(request.url.path(), "/backups/docs/notes.txt");
/// assert_eq!(request.headers.get("if-none-match"), Some("*"));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait PrepareRequest {
    /// Build the request without reading the action's credentials.
    #[must_use]
    fn unsigned_request(&self) -> UnsignedRequest<'_>;
}

/// An S3 request ready for signing.
#[derive(Debug, Clone)]
pub struct UnsignedRequest<'a> {
    /// HTTP method to sign.
    pub method: Method,
    /// AWS signing region. The signing service name is `s3`.
    pub region: &'a str,
    /// Additional headers to sign. The signer derives `host` from the URL.
    pub headers: &'a Map<'a>,
    /// Target URL with the full query string.
    pub url: Url,
}

pub(super) trait BuildRequest {
    fn request_parts(&self) -> RequestParts<'_>;
}

impl<T: BuildRequest> PrepareRequest for T {
    fn unsigned_request(&self) -> UnsignedRequest<'_> {
        let parts = self.request_parts();
        UnsignedRequest {
            method: parts.method,
            region: parts.region,
            headers: parts.headers,
            url: parts.into_url(),
        }
    }
}

pub(super) struct RequestParts<'a> {
    method: Method,
    region: &'a str,
    headers: &'a Map<'a>,
    pub(super) base_url: Url,
    pub(super) query: Query<'a>,
}

pub(super) struct Query<'a> {
    operation: Vec<(&'static str, Cow<'a, str>)>,
    custom: &'a Map<'a>,
}

impl Query<'_> {
    pub(super) fn operation_pairs(&self) -> impl Iterator<Item = (&str, &str)> + Clone {
        self.operation.iter().map(|(k, v)| (*k, v.as_ref()))
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&str, &str)> + Clone {
        SortingIterator::new(self.operation_pairs(), self.custom.iter())
    }
}

impl<'a> RequestParts<'a> {
    pub(super) const fn new(
        method: Method,
        base_url: Url,
        region: &'a str,
        headers: &'a Map<'a>,
        custom: &'a Map<'a>,
        operation: Vec<(&'static str, Cow<'a, str>)>,
    ) -> Self {
        Self {
            method,
            base_url,
            region,
            headers,
            query: Query { operation, custom },
        }
    }

    pub(super) fn into_url(self) -> Url {
        let mut url = self.base_url;
        let mut query = String::new();
        canonical_query_string(self.query.iter(), &mut query).expect("writing to a String");
        url.set_query(if query.is_empty() { None } else { Some(&query) });
        url
    }

    pub(super) fn presign(
        self,
        credentials: &Credentials,
        expires_in: Duration,
        time: &Timestamp,
    ) -> Url {
        sign(
            time,
            self.method,
            self.base_url,
            credentials.key(),
            credentials.secret(),
            credentials.token(),
            self.region,
            expires_in.as_secs(),
            self.query.iter(),
            self.headers.iter(),
        )
    }
}
