pub mod common;

use ::function_name::named;
use common::aws::S3TestClient;
use common::fixtures;
use s3_archiver::{Compression, S3Object};

#[tokio::test]
#[named]
async fn test_put_get() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let test_bucket = "test-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());
    let src_key = fixtures::gen_random_file_name(&mut rng);
    let dst_key = fixtures::gen_random_file_name(&mut rng);
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
        (5 * bytesize::MIB).try_into().unwrap(),
        2,
        &dst,
    )
    .await
    .expect("Expected zip creation");

    assert!(fixtures::check_object_exists(&s3_client, &dst)
        .await
        .unwrap());
}

#[tokio::test]
#[named]
async fn test_check_zip_stored() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let mut rng = fixtures::seeded_rng(function_name!());
    let args = fixtures::CheckZipArgs::seeded_args(&mut rng, 10, None);
    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .unwrap();
}

#[tokio::test]
#[named]
async fn test_check_zip_deflate() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let mut rng = fixtures::seeded_rng(function_name!());
    let args = fixtures::CheckZipArgs {
        compression: Compression::Deflate,
        ..fixtures::CheckZipArgs::seeded_args(&mut rng, 1, None)
    };

    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .unwrap();
}

#[tokio::test]
#[named]
async fn test_check_zip_bzip() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let mut rng = fixtures::seeded_rng(function_name!());
    let args = fixtures::CheckZipArgs {
        compression: Compression::Bzip,
        ..fixtures::CheckZipArgs::seeded_args(&mut rng, 1, None)
    };

    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .unwrap();
}

#[tokio::test]
#[named]
async fn test_check_zip_lzma() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let mut rng = fixtures::seeded_rng(function_name!());
    let args = fixtures::CheckZipArgs {
        compression: Compression::Lzma,
        ..fixtures::CheckZipArgs::seeded_args(&mut rng, 1, None)
    };

    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .unwrap();
}

#[tokio::test]
#[named]
async fn test_check_zip_zstd() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let mut rng = fixtures::seeded_rng(function_name!());
    let args = fixtures::CheckZipArgs {
        compression: Compression::Zstd,
        ..fixtures::CheckZipArgs::seeded_args(&mut rng, 1, None)
    };

    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .unwrap();
}

#[tokio::test]
#[named]
async fn test_check_zip_xz() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let mut rng = fixtures::seeded_rng(function_name!());
    let args = fixtures::CheckZipArgs {
        compression: Compression::Xz,
        ..fixtures::CheckZipArgs::seeded_args(&mut rng, 1, None)
    };

    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .unwrap();
}

#[tokio::test]
#[named]
async fn test_check_zip_with_dirs() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let mut rng = fixtures::seeded_rng(function_name!());
    let args = fixtures::CheckZipArgs::seeded_args(&mut rng, 10, Some(&["dir_one", "dir_two"]));
    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .unwrap();
}

#[tokio::test]
#[named]
async fn test_check_zip_with_prefix() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let mut rng = fixtures::seeded_rng(function_name!());
    let args = fixtures::CheckZipArgs {
        prefix_to_strip: Some("prefix/"),
        ..fixtures::CheckZipArgs::seeded_args(&mut rng, 10, Some(&["prefix"]))
    };

    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_check_zip_with_prefix_that_removes_key() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    //In this case we want a very specific key
    let args = fixtures::CheckZipArgs {
        src_keys: vec![
            //Yes this is strange key but S3 allows it.
            "prefix/src/".into(),
        ],
        prefix_to_strip: Some("prefix/src/"),
        ..fixtures::CheckZipArgs::default()
    };

    assert!(fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .is_err());
}

#[tokio::test]
async fn test_check_zip_with_prefix_that_starts_with_slash() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let args = fixtures::CheckZipArgs {
        prefix_to_strip: Some("/prefix/src/"),
        ..fixtures::CheckZipArgs::default()
    };

    assert!(fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .is_err());
}

#[tokio::test]
async fn test_check_zip_with_prefix_that_does_not_end_with_slash() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let args = fixtures::CheckZipArgs {
        prefix_to_strip: Some("prefix/src"),
        ..fixtures::CheckZipArgs::default()
    };

    assert!(fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .is_err());
}
