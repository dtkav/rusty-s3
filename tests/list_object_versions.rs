use std::time::Duration;

use rusty_s3::actions::{ListObjectVersions, S3Action as _};
use rusty_s3::actions::{VersioningStatus};

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

#[tokio::test]
async fn list_object_versions_multiple() {
    let (bucket, credentials, client) = common::bucket().await;

    // Enable versioning for the bucket
    let action = bucket.put_bucket_versioning(&credentials, VersioningStatus::Enabled);
    let url = action.sign(Duration::from_secs(60));
    let body = action.body();
    client
        .put(url)
        .body(body)
        .send()
        .await
        .expect("send PutBucketVersioning")
        .error_for_status()
        .expect("PutBucketVersioning unexpected status code");

    // Upload multiple versions of the same key
    let key = "multi.txt";
    for data in ["first", "second", "third"] {
        let action = bucket.put_object(Some(&credentials), key);
        let url = action.sign(Duration::from_secs(60));
        client
            .put(url)
            .body(data.as_bytes().to_vec())
            .send()
            .await
            .expect("send PutObject")
            .error_for_status()
            .expect("PutObject unexpected status code");
    }

    // List object versions
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

    let count = versions.versions.iter().filter(|v| v.key == key).count();
    assert_eq!(count, 3);
    assert!(versions.delete_markers.is_empty());
}
