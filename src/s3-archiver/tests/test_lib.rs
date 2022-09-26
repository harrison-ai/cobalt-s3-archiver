mod common;

use aws_sdk_s3::types::ByteStream;
use common::aws::s3_client;
use common::aws::S3TestClient;
use common::fixtures;
use common::localstack::LocalStack;
use futures::prelude::*;
use s3_archiver::aws::AsyncMultipartUpload;
use s3_archiver::{Compression, S3Object};
use testcontainers::clients;

#[tokio::test]
async fn test_put_get() {
    #[cfg(feature = "test_containers")]
    let test_client = S3TestClient::TestContainer(clients::Cli::default());
    #[cfg(not(feature = "test_containers"))]
    let test_client = S3TestClient::DockerCompose;

    let (_container, s3_client) = test_client.client().await;

    let test_bucket = "test-bucket";
    let src_key = "src-file.txt";
    let dst_key = "dst-file.zip";

    s3_client
        .create_bucket()
        .bucket(test_bucket)
        .send()
        .await
        .expect("Error creating bucket");

    s3_client
        .put_object()
        .bucket(test_bucket)
        .key(src_key)
        .body(ByteStream::from(vec![1, 2, 3, 4, 5]))
        .send()
        .await
        .expect("Expected to create source file");

    let src = Ok(S3Object::new(test_bucket, src_key));
    let dst: S3Object = S3Object::new(test_bucket, dst_key);
    s3_archiver::create_zip(
        &s3_client,
        vec![src].into_iter(),
        "",
        Compression::Stored,
        &dst,
    )
    .await
    .expect("Expected zip creation");

    s3_client
        .head_object()
        .bucket(test_bucket)
        .key(dst_key)
        .send()
        .await
        .expect("Expceted dst key to exist");
}

#[tokio::test]
async fn test_async_multipart_upload() {
    #[cfg(feature = "test_containers")]
    let test_client = S3TestClient::TestContainer(clients::Cli::default());
    #[cfg(not(feature = "test_containers"))]
    let test_client = S3TestClient::DockerCompose;

    let (_container, s3_client) = test_client.client().await;

    let test_bucket = "test-bucket";
    fixtures::create_bucket(&s3_client, test_bucket)
        .await
        .expect("Expected to create bucket");
    let test_key = "test-output.txt";

    let mut upload =
        AsyncMultipartUpload::new(&s3_client, test_bucket, test_key, 5 * 1024_usize.pow(2))
            .await
            .expect("Expceted to create mulipart");

    upload
        .write_all(b"test content")
        .await
        .expect("Expected to write");
    upload.close().await.expect("Upload should have closed");
}
