use std::time::Duration;

use jiff::Timestamp;
use rusty_s3::actions::{CreateBucket, DeleteBucket};
use rusty_s3::{
    Bucket, Credentials, Method, PrepareRequest as _, S3Action as _, UnsignedRequest, UrlStyle,
};
use url::Url;

fn bucket(style: UrlStyle) -> Bucket {
    let endpoint = "https://storage.example:9443".parse().unwrap();
    Bucket::new(endpoint, style, "docs", "us-west-2").unwrap()
}

fn presign(request: &UnsignedRequest<'_>, credentials: &Credentials, time: &Timestamp) -> Url {
    let query: Vec<_> = request.url.query_pairs().collect();
    rusty_s3::signing::sign(
        time,
        request.method,
        request.url.clone(),
        credentials.key(),
        credentials.secret(),
        credentials.token(),
        request.region,
        60,
        query.iter().map(|(k, v)| (k.as_ref(), v.as_ref())),
        request.headers.iter(),
    )
}

#[test]
fn conditional_put_matches_local_signature() {
    let bucket = bucket(UrlStyle::Path);
    let credentials = Credentials::new_with_token("key", "secret", "session");
    let time = Timestamp::from_second(1_369_353_600).unwrap();
    let put = |credentials| {
        let mut action = bucket.put_object(credentials, "drafts/📘 +.txt");
        action.headers_mut().insert("if-match", "\"revision-1\"");
        action.headers_mut().insert("content-type", "text/plain");
        action.query_mut().insert("tag", "a +/%");
        action
    };

    let action = put(None);
    let request = action.unsigned_request();
    assert_eq!(request.method, Method::Put);
    assert_eq!(request.region, "us-west-2");
    assert_eq!(request.headers.get("if-match"), Some("\"revision-1\""));
    assert_eq!(request.headers.get("content-type"), Some("text/plain"));
    assert_eq!(
        request.url.as_str(),
        "https://storage.example:9443/docs/drafts/%F0%9F%93%98%20%2B.txt?tag=a%20%2B%2F%25"
    );
    let local = put(Some(&credentials));
    assert_eq!(local.unsigned_request().url, request.url);
    assert_eq!(
        presign(&request, &credentials, &time),
        local.sign_with_time(Duration::from_secs(60), &time)
    );
}

#[test]
fn upload_part_preserves_duplicate_query_keys() {
    let bucket = bucket(UrlStyle::VirtualHost);
    let mut upload = bucket.upload_part(None, "snapshot", 7, "id+/=");
    upload.query_mut().insert("uploadId", "additional");
    let request = upload.unsigned_request();
    assert_eq!(request.method, Method::Put);
    assert_eq!(
        request.url.as_str(),
        "https://docs.storage.example:9443/snapshot?partNumber=7&uploadId=additional&uploadId=id%2B%2F%3D"
    );
}

#[test]
fn create_and_delete_bucket_without_credentials() {
    let bucket = bucket(UrlStyle::VirtualHost);
    let create = CreateBucket::new_unsigned(&bucket);
    let delete = DeleteBucket::new_unsigned(&bucket);
    assert_eq!(create.unsigned_request().method, Method::Put);
    assert_eq!(delete.unsigned_request().method, Method::Delete);
    assert_eq!(
        create.unsigned_request().url.as_str(),
        "https://docs.storage.example:9443/"
    );
    assert_eq!(delete.unsigned_request().url, create.unsigned_request().url);
}

#[cfg(feature = "full")]
#[test]
fn bucket_subresources_and_list_parameters() {
    use rusty_s3::actions::{GetBucketPolicy, PutBucketVersioning, VersioningStatus};
    let bucket = bucket(UrlStyle::Path);
    let policy = GetBucketPolicy::new(&bucket, None);
    let versioning = PutBucketVersioning::new_unsigned(&bucket, VersioningStatus::Enabled);
    let mut list = bucket.list_objects_v2(None);
    list.with_prefix("relay/");
    list.with_continuation_token("next+page");
    assert_eq!(policy.unsigned_request().url.query(), Some("policy="));
    assert_eq!(
        versioning.unsigned_request().url.query(),
        Some("versioning=")
    );
    assert_eq!(
        list.unsigned_request().url.query(),
        Some("continuation-token=next%2Bpage&encoding-type=url&list-type=2&prefix=relay%2F")
    );
}
