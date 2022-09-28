mod common;

use common::aws::S3TestClient;
use common::fixtures;
use s3_archiver::{Compression, S3Object};

#[tokio::test]
async fn test_put_get() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let test_bucket = "test-bucket";
    let src_key = "src-file.txt";
    let dst_key = "dst-file.zip";
    let src = S3Object::new(test_bucket, src_key);
    fixtures::create_bucket(&s3_client, test_bucket)
        .await
        .unwrap();
    fixtures::create_random_file(&s3_client, &src, 10)
        .await
        .unwrap();

    let dst: S3Object = S3Object::new(test_bucket, dst_key);
    s3_archiver::create_zip(
        &s3_client,
        vec![Ok(src)].into_iter(),
        None,
        Compression::Stored,
        &dst,
    )
    .await
    .expect("Expected zip creation");

    assert!(fixtures::check_object_exists(&s3_client, &dst)
        .await
        .unwrap());
}

#[tokio::test]
async fn test_check_zip() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let dst_obj = S3Object::new("dst-bucket", "dst_check_file.zip");
    let prefix_to_strip = Option::<&str>::None;
    let src_bucket = "src-bucket";
    let src_files = ["src-file_1.txt", "src-file_2.txt"];
    let file_size = 1024_usize.pow(2);
    let compression = Compression::Stored;

    fixtures::create_and_validate_zip(
        &s3_client,
        &dst_obj,
        src_bucket,
        &src_files,
        prefix_to_strip,
        file_size,
        compression,
    )
    .await
    .unwrap();
}
