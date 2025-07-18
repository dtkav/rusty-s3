use std::borrow::Cow;
use std::io::{BufReader, Read};
use std::iter;
use std::time::Duration;

use jiff::Timestamp;
use serde::Deserialize;
use url::Url;

use crate::actions::Method;
use crate::actions::S3Action;
use crate::signing::sign;
use crate::sorting_iter::SortingIterator;
use crate::{Bucket, Credentials, Map};

/// List all versions of objects in the bucket.
///
/// If `next_key_marker` or `next_version_id_marker` is `Some` the response is
/// truncated, and the rest of the list can be retrieved by reusing the
/// `ListObjectVersions` action but with `key-marker` and `version-id-marker`
/// set to the values returned in the previous response.
///
/// Find out more about `ListObjectVersions` from the [AWS API Reference][api]
///
/// [api]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html
#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone)]
pub struct ListObjectVersions<'a> {
    bucket: &'a Bucket,
    credentials: Option<&'a Credentials>,

    query: Map<'a>,
    headers: Map<'a>,
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone, Deserialize)]
pub struct ListObjectVersionsResponse {
    #[serde(rename = "Version", default)]
    pub versions: Vec<ObjectVersion>,
    #[serde(rename = "DeleteMarker", default)]
    pub delete_markers: Vec<DeleteMarker>,
    #[serde(rename = "CommonPrefixes", default)]
    pub common_prefixes: Vec<CommonPrefixes>,
    #[serde(rename = "MaxKeys")]
    pub max_keys: Option<u16>,
    #[serde(rename = "NextKeyMarker")]
    pub next_key_marker: Option<String>,
    #[serde(rename = "NextVersionIdMarker")]
    pub next_version_id_marker: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ObjectVersion {
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    #[serde(rename = "Owner")]
    pub owner: Option<ListObjectsOwner>,
    #[serde(rename = "Size")]
    pub size: u64,
    #[serde(rename = "StorageClass")]
    pub storage_class: Option<String>,
    #[serde(rename = "VersionId")]
    pub version_id: String,
    #[serde(rename = "IsLatest")]
    pub is_latest: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeleteMarker {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "VersionId")]
    pub version_id: String,
    #[serde(rename = "IsLatest")]
    pub is_latest: bool,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    #[serde(rename = "Owner")]
    pub owner: Option<ListObjectsOwner>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListObjectsOwner {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "DisplayName")]
    pub display_name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CommonPrefixes {
    #[serde(rename = "Prefix")]
    pub prefix: String,
}

impl<'a> ListObjectVersions<'a> {
    #[must_use]
    pub fn new(bucket: &'a Bucket, credentials: Option<&'a Credentials>) -> Self {
        let mut query = Map::new();
        query.insert("encoding-type", "url");

        Self {
            bucket,
            credentials,
            query,
            headers: Map::new(),
        }
    }

    /// Limits the response to keys that begin with the specified prefix.
    pub fn with_prefix(&mut self, prefix: impl Into<Cow<'a, str>>) {
        self.query_mut().insert("prefix", prefix);
    }

    /// Sets the maximum number of keys returned in the response.
    pub fn with_max_keys(&mut self, max_keys: usize) {
        self.query_mut().insert("max-keys", max_keys.to_string());
    }

    /// Specify the key to start with when listing.
    pub fn with_key_marker(&mut self, key: impl Into<Cow<'a, str>>) {
        self.query_mut().insert("key-marker", key);
    }

    /// Specify the object version you want to start listing from.
    pub fn with_version_id_marker(&mut self, version: impl Into<Cow<'a, str>>) {
        self.query_mut().insert("version-id-marker", version);
    }

    /// A delimiter is a character that you use to group keys.
    pub fn with_delimiter(&mut self, delimiter: impl Into<Cow<'a, str>>) {
        self.query_mut().insert("delimiter", delimiter);
    }

    /// Parse the XML response from S3 into a struct.
    ///
    /// # Errors
    ///
    /// Returns an error if the XML response could not be parsed.
    pub fn parse_response(
        s: impl AsRef<[u8]>,
    ) -> Result<ListObjectVersionsResponse, quick_xml::DeError> {
        Self::parse_response_from_reader(&mut s.as_ref())
    }

    /// Parse the XML response from S3 into a struct.
    ///
    /// # Errors
    ///
    /// Returns an error if the XML response could not be parsed.
    pub fn parse_response_from_reader(
        s: impl Read,
    ) -> Result<ListObjectVersionsResponse, quick_xml::DeError> {
        let mut parsed: ListObjectVersionsResponse =
            quick_xml::de::from_reader(BufReader::new(s))?;

        for version in &mut parsed.versions {
            if let Some(owner) = &version.owner {
                if owner.id.is_empty() && owner.display_name.is_empty() {
                    version.owner = None;
                }
            }
        }
        for marker in &mut parsed.delete_markers {
            if let Some(owner) = &marker.owner {
                if owner.id.is_empty() && owner.display_name.is_empty() {
                    marker.owner = None;
                }
            }
        }

        Ok(parsed)
    }
}

impl<'a> S3Action<'a> for ListObjectVersions<'a> {
    const METHOD: Method = Method::Get;

    fn query_mut(&mut self) -> &mut Map<'a> {
        &mut self.query
    }

    fn headers_mut(&mut self) -> &mut Map<'a> {
        &mut self.headers
    }

    fn sign_with_time(&self, expires_in: Duration, time: &Timestamp) -> Url {
        let url = self.bucket.base_url().clone();
        let query = SortingIterator::new(iter::once(("versions", "1")), self.query.iter());

        match self.credentials {
            Some(credentials) => sign(
                time,
                Self::METHOD,
                url,
                credentials.key(),
                credentials.secret(),
                credentials.token(),
                self.bucket.region(),
                expires_in.as_secs(),
                query,
                self.headers.iter(),
            ),
            None => crate::signing::util::add_query_params(url, query),
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::{Bucket, UrlStyle};

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

        let mut action = ListObjectVersions::new(&bucket, None);
        action.with_prefix("duck");
        let url = action.sign(expires_in);
        let expected = "https://examplebucket.s3.amazonaws.com/?encoding-type=url&prefix=duck&versions=1";
        assert_eq!(expected, url.as_str());
    }

    #[test]
    fn parse() {
        let input = r#"
        <?xml version="1.0" encoding="UTF-8"?>
        <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>bucket</Name>
            <Prefix>my</Prefix>
            <KeyMarker></KeyMarker>
            <VersionIdMarker></VersionIdMarker>
            <MaxKeys>5</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Version>
                <Key>my-image.jpg</Key>
                <VersionId>3/L4kqtJl40Nr8X8gdRQBpUMLUo</VersionId>
                <IsLatest>true</IsLatest>
                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                <ETag>"fba9dede5f27731c9771645a39863328"</ETag>
                <Size>434234</Size>
                <Owner>
                    <ID></ID>
                    <DisplayName></DisplayName>
                </Owner>
                <StorageClass>STANDARD</StorageClass>
            </Version>
            <DeleteMarker>
                <Key>old-file.jpg</Key>
                <VersionId>abc123</VersionId>
                <IsLatest>false</IsLatest>
                <LastModified>2009-10-11T17:50:30.000Z</LastModified>
                <Owner>
                    <ID></ID>
                    <DisplayName></DisplayName>
                </Owner>
            </DeleteMarker>
            <EncodingType>url</EncodingType>
        </ListVersionsResult>
        "#;

        let parsed = ListObjectVersions::parse_response(input).unwrap();
        assert_eq!(parsed.versions.len(), 1);
        assert_eq!(parsed.delete_markers.len(), 1);
        let v = &parsed.versions[0];
        assert_eq!(v.key, "my-image.jpg");
        assert_eq!(v.version_id, "3/L4kqtJl40Nr8X8gdRQBpUMLUo");
        assert!(v.owner.is_none());
    }
}
