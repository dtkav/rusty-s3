use std::time::Duration;

use rusty_s3::actions::{ListObjectVersions, S3Action as _};

mod common;

#[tokio::test]
async fn list_object_versions_empty() {
    let (bucket, credentials, client) = common::bucket().await;

    let action = bucket.list_object_versions(Some(&credentials));
    let url = action.sign(Duration::from_secs(60));
    let resp = client
        .get(url)
        .send()
        .await
        .expect("send ListObjectVersions")
        .error_for_status()
        .expect("ListObjectVersions unexpected status code");
    let text = resp
        .text()
        .await
        .expect("ListObjectVersions read response body");
    let versions =
        ListObjectVersions::parse_response(&text).expect("ListObjectVersions parse response");

    assert!(versions.versions.is_empty());
    assert!(versions.delete_markers.is_empty());
}
