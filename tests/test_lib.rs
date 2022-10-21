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
    let archiver = s3_archiver::Archiver::builder()
        .compression(Compression::Stored)
        .build();
    archiver
        .create_zip(&s3_client, vec![Ok(src)].into_iter(), &dst, None)
        .await
        .expect("Expected zip creation");

    assert!(fixtures::check_object_exists(&s3_client, &dst)
        .await
        .unwrap());
}

#[tokio::test]
#[named]
async fn test_put_get_with_manifest() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let test_bucket = "test-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());
    let src_key = fixtures::gen_random_file_name(&mut rng);
    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let manifest_key = fixtures::gen_random_file_name(&mut rng);
    let src = S3Object::new(test_bucket, src_key);
    let manfest_object = S3Object::new(test_bucket, manifest_key);
    fixtures::create_bucket(&s3_client, test_bucket)
        .await
        .unwrap();
    fixtures::create_random_file(&s3_client, &src, 10)
        .await
        .unwrap();

    let dst: S3Object = S3Object::new(test_bucket, dst_key);
    let archiver = s3_archiver::Archiver::builder()
        .compression(Compression::Stored)
        .build();
    archiver
        .create_zip(
            &s3_client,
            vec![Ok(src)].into_iter(),
            &dst,
            Some(&manfest_object),
        )
        .await
        .expect("Expected zip creation");

    assert!(fixtures::check_object_exists(&s3_client, &dst)
        .await
        .unwrap());
    assert!(fixtures::check_object_exists(&s3_client, &manfest_object)
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

#[tokio::test]
#[named]
async fn test_validate_zip_entry_whole_file() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let mut rng = fixtures::seeded_rng(function_name!());

    use Compression::*;
    let compressions = vec![Deflate, Bzip, Lzma, Zstd, Xz, Stored];
    for compression in compressions {
        let args = fixtures::CheckZipArgs {
            compression,
            ..fixtures::CheckZipArgs::seeded_args(&mut rng, 10, None)
        };

        fixtures::create_and_validate_zip(&s3_client, &args)
            .await
            .expect("Error creating and validating with {compression:?}");
        s3_archiver::validate_zip_entry_bytes(
            &s3_client,
            args.manifest_file.as_ref().unwrap(),
            &args.dst_obj,
        )
        .await
        .expect("Error creating and validating zip entry bytes {compression:?}");
        s3_archiver::validate_zip_central_dir(
            &s3_client,
            args.manifest_file.as_ref().unwrap(),
            &args.dst_obj,
        )
        .await
        .expect("Error creating and validating zip entry central-directory {compression:?}");
    }
}

#[tokio::test]
#[named]
async fn test_validate_zip_entry_streamed_file() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let mut rng = fixtures::seeded_rng(function_name!());

    use Compression::*;
    let compressions = vec![Deflate, Bzip, Lzma, Zstd, Xz, Stored];
    for compression in compressions {
        let args = fixtures::CheckZipArgs {
            data_descriptors: true,
            compression,
            ..fixtures::CheckZipArgs::seeded_args(&mut rng, 10, None)
        };

        fixtures::create_and_validate_zip(&s3_client, &args)
            .await
            .expect("Error creating and validating with {compression:?}");
        let bytes_result = s3_archiver::validate_zip_entry_bytes(
            &s3_client,
            args.manifest_file.as_ref().unwrap(),
            &args.dst_obj,
        )
        .await;

        match compression {
            Stored => assert!(bytes_result.is_err(), "Streaming read of zip with no compression written with Data Descriptions should fail"),
            _ => assert!(bytes_result.is_ok(), "Streaming read of zip with {compression:?} entries written with Data Descriptions should not fail")
       }

        s3_archiver::validate_zip_central_dir(
            &s3_client,
            args.manifest_file.as_ref().unwrap(),
            &args.dst_obj,
        )
        .await
        .expect("Error creating and validating zip entry central-directory {compression:?}");
    }
}
