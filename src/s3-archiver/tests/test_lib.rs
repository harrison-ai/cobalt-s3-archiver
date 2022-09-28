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
async fn test_check_zip_stored() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let args = fixtures::CheckZipArgs::default();
    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_check_zip_deflate() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let args = fixtures::CheckZipArgs{
        compression: Compression::Deflate,
        ..fixtures::CheckZipArgs::default()
    };
    
    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .unwrap();
}


#[tokio::test]
async fn test_check_zip_bzip() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let args = fixtures::CheckZipArgs{
        compression: Compression::Bzip,
        ..fixtures::CheckZipArgs::default()
    };
    
    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .unwrap();
}


#[tokio::test]
async fn test_check_zip_lzma() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let args = fixtures::CheckZipArgs{
        compression: Compression::Lzma,
        ..fixtures::CheckZipArgs::default()
    };
    
    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .unwrap();
}


#[tokio::test]
async fn test_check_zip_zstd() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let args = fixtures::CheckZipArgs{
        compression: Compression::Zstd,
        ..fixtures::CheckZipArgs::default()
    };
    
    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_check_zip_xz() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let args = fixtures::CheckZipArgs{
        compression: Compression::Xz,
        ..fixtures::CheckZipArgs::default()
    };
    
    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .unwrap();
}

