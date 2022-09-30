pub mod common;

use ::function_name::named;
use common::aws::S3TestClient;
use common::fixtures;
use futures::prelude::*;
use s3_archiver::aws::AsyncMultipartUpload;
use s3_archiver::S3Object;

#[tokio::test]
#[named]
async fn test_put_single_part() {
    let test_client = S3TestClient::default();
    let (_container, client) = test_client.client().await;
    let test_bucket = "test-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());
    let dst_key = fixtures::gen_random_file_name(&mut rng);

    fixtures::create_bucket(&client, test_bucket).await.unwrap();
    let buffer_len = 1024_usize.pow(2);

    let mut upload =
        AsyncMultipartUpload::new(&client, test_bucket, &dst_key, 5_usize * 1024_usize.pow(2))
            .await
            .unwrap();
    upload.write_all(&vec![0; buffer_len]).await.unwrap();
    upload.close().await.unwrap();
    let body = fixtures::fetch_bytes(&client, &S3Object::new(test_bucket, &dst_key))
        .await
        .unwrap();
    assert_eq!(body.len(), buffer_len);
}

#[tokio::test]
#[named]
async fn test_put_10mb() {
    let test_client = S3TestClient::default();
    let (_container, client) = test_client.client().await;
    let test_bucket = "test-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());
    let dst_key = fixtures::gen_random_file_name(&mut rng);

    fixtures::create_bucket(&client, test_bucket).await.unwrap();
    let data_len = 10 * 1024_usize.pow(2);

    let mut upload =
        AsyncMultipartUpload::new(&client, test_bucket, &dst_key, 5 * 1024_usize.pow(2))
            .await
            .unwrap();
    upload.write_all(&vec![0; data_len]).await.unwrap();
    upload.close().await.unwrap();
    let body = fixtures::fetch_bytes(&client, &S3Object::new(test_bucket, &dst_key))
        .await
        .unwrap();
    assert_eq!(body.len(), data_len);
}

#[tokio::test]
#[named]
async fn test_put_14mb() {
    let test_client = S3TestClient::default();
    let (_container, client) = test_client.client().await;
    let test_bucket = "test-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());
    let dst_key = fixtures::gen_random_file_name(&mut rng);

    fixtures::create_bucket(&client, test_bucket).await.unwrap();

    let mut upload =
        AsyncMultipartUpload::new(&client, test_bucket, &dst_key, 5 * 1024_usize.pow(2))
            .await
            .unwrap();

    let data_len = 14 * 1024_usize.pow(2);

    upload.write_all(&vec![0; data_len]).await.unwrap();
    upload.close().await.unwrap();
}

#[tokio::test]
#[named]
#[cfg(feature = "test_containers")]
async fn test_fail_write() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    let test_bucket = "test-bucket";

    let mut rng = fixtures::seeded_rng(function_name!());
    let dst_key = fixtures::gen_random_file_name(&mut rng);

    fixtures::create_bucket(&client, test_bucket).await.unwrap();

    let mut upload =
        AsyncMultipartUpload::new(&client, test_bucket, &dst_key, 5 * 1024_usize.pow(2))
            .await
            .unwrap();

    let data_len = 6 * 1024_usize.pow(2);

    drop(container);
    upload.write_all(&vec![0; data_len]).await.unwrap();
    assert!(upload.flush().await.is_err());
}

#[tokio::test]
#[named]
#[cfg(feature = "test_containers")]
async fn test_fail_close() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    let test_bucket = "test-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());
    let dst_key = fixtures::gen_random_file_name(&mut rng);

    fixtures::create_bucket(&client, test_bucket).await.unwrap();

    let mut upload =
        AsyncMultipartUpload::new(&client, test_bucket, &dst_key, 5 * 1024_usize.pow(2))
            .await
            .unwrap();

    let data_len = 6 * 1024_usize.pow(2);

    drop(container);
    upload.write_all(&vec![0; data_len]).await.unwrap();
    assert!(upload.close().await.is_err());
}
