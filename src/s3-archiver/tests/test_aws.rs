mod common;

use common::aws::S3TestClient;
use common::fixtures;
use futures::prelude::*;
use s3_archiver::aws::AsyncMultipartUpload;
#[cfg(feature = "test_containers")]
use testcontainers::clients;

#[tokio::test]
async fn test_put_single_part() {
    #[cfg(feature = "test_containers")]
    let test_client = S3TestClient::TestContainer(clients::Cli::default());
    #[cfg(not(feature = "test_containers"))]
    let test_client = S3TestClient::DockerCompose;

    let (_container, client) = test_client.client().await;
    let test_bucket = "test-bucket";
    let dst_key = "dst-file.zip";

    fixtures::create_bucket(&client, test_bucket).await.unwrap();
    let buffer_len = 1024_usize.pow(2);

    let mut upload =
        AsyncMultipartUpload::new(&client, test_bucket, dst_key, 5_usize * 1024_usize.pow(2))
            .await
            .unwrap();
    upload.write_all(&vec![0; buffer_len]).await.unwrap();
    upload.close().await.unwrap();

    let result = client
        .get_object()
        .bucket(test_bucket)
        .key(dst_key)
        .send()
        .await
        .expect("Expceted dst key to exist");

    let body_len = result.body.collect().await.unwrap().into_bytes().len();
    assert_eq!(body_len, buffer_len);
}
#[tokio::test]
async fn test_put_10mb() {
    #[cfg(feature = "test_containers")]
    let test_client = S3TestClient::TestContainer(clients::Cli::default());
    #[cfg(not(feature = "test_containers"))]
    let test_client = S3TestClient::DockerCompose;

    let (_container, client) = test_client.client().await;
    let test_bucket = "test-bucket";
    let dst_key = "dst-file.zip";

    fixtures::create_bucket(&client, test_bucket).await.unwrap();

    let mut upload =
        AsyncMultipartUpload::new(&client, test_bucket, dst_key, 5_usize * 1024_usize.pow(2))
            .await
            .unwrap();
    upload
        .write_all(&vec![0; 10 * 1024_usize.pow(2)])
        .await
        .unwrap();
    upload.close().await.unwrap();

    let result = client
        .get_object()
        .bucket(test_bucket)
        .key(dst_key)
        .send()
        .await
        .expect("Expceted dst key to exist");

    let body_len = result.body.collect().await.unwrap().into_bytes().len();
    assert_eq!(body_len, 10 * 1024_usize.pow(2));
}

#[tokio::test]
async fn test_put_7mb() {
    #[cfg(feature = "test_containers")]
    let test_client = S3TestClient::TestContainer(clients::Cli::default());
    #[cfg(not(feature = "test_containers"))]
    let test_client = S3TestClient::DockerCompose;

    let (_container, client) = test_client.client().await;
    let test_bucket = "test-bucket";
    let dst_key = "dst-file.zip";

    fixtures::create_bucket(&client, test_bucket).await.unwrap();

    let mut upload =
        AsyncMultipartUpload::new(&client, test_bucket, dst_key, 5 * 1025_usize.pow(2))
            .await
            .unwrap();

    let data_len = 14 * 1024_usize.pow(2);

    upload.write_all(&vec![0; data_len]).await.unwrap();
    upload.close().await.unwrap();

    let result = client
        .get_object()
        .bucket(test_bucket)
        .key(dst_key)
        .send()
        .await
        .expect("Expceted dst key to exist");

    let body_len = result.body.collect().await.unwrap().into_bytes().len();
    assert_eq!(body_len, data_len);
}


#[tokio::test]
#[cfg(feature = "test_containers")]
async fn test_fail_write() {
    let test_client = S3TestClient::TestContainer(clients::Cli::default());
    let (container, client) = test_client.client().await;
    let test_bucket = "test-bucket";
    let dst_key = "dst-file.zip";

    fixtures::create_bucket(&client, test_bucket).await.unwrap();

    let mut upload =
        AsyncMultipartUpload::new(&client, test_bucket, dst_key, 5 * 1025_usize.pow(2))
            .await
            .unwrap();

    let data_len = 6 * 1024_usize.pow(2);

    drop(container);
    upload.write(&vec![0; data_len]).await.unwrap();
    assert!(upload.flush().await.is_err());
}


#[tokio::test]
#[cfg(feature = "test_containers")]
async fn test_fail_close() {
    let test_client = S3TestClient::TestContainer(clients::Cli::default());
    let (container, client) = test_client.client().await;
    let test_bucket = "test-bucket";
    let dst_key = "dst-file.zip";

    fixtures::create_bucket(&client, test_bucket).await.unwrap();

    let mut upload =
        AsyncMultipartUpload::new(&client, test_bucket, dst_key, 5 * 1025_usize.pow(2))
            .await
            .unwrap();

    let data_len = 6 * 1024_usize.pow(2);

    drop(container);
    upload.write(&vec![0; data_len]).await.unwrap();
    assert!(upload.close().await.is_err());
}
