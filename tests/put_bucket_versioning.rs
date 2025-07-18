use std::time::Duration;

use rusty_s3::actions::{PutBucketVersioning, S3Action as _, VersioningStatus};

mod common;

#[tokio::test]
async fn enable_versioning() {
    let (bucket, credentials, client) = common::bucket().await;

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
}
