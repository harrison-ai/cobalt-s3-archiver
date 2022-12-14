pub mod common;

use ::function_name::named;
use bytesize::MIB;
use cobalt_aws::s3::S3Object;
use cobalt_s3_archiver::{
    validate_manifest_file, validate_zip_central_dir, validate_zip_entry_bytes, Archiver,
    Compression, ManifestEntry,
};
use common::aws::S3TestClient;
use common::fixtures;
use futures::prelude::*;
use rand::Rng;
use std::io::prelude::*;

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
    let archiver = Archiver::builder().compression(Compression::Stored).build();
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
    let archiver = Archiver::builder().compression(Compression::Stored).build();
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
        validate_zip_entry_bytes(
            &s3_client,
            args.manifest_file.as_ref().unwrap(),
            &args.dst_obj,
        )
        .await
        .expect("Error creating and validating zip entry bytes {compression:?}");
        validate_zip_central_dir(
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

        let bytes_result = validate_zip_entry_bytes(
            &s3_client,
            args.manifest_file.as_ref().unwrap(),
            &args.dst_obj,
        )
        .await;

        match compression {
            Stored => assert!(bytes_result.is_err(), "Streaming read of zip with no compression written with Data Descriptions should fail"),
            _ => {
                assert!(bytes_result.is_ok(), "Streaming read of zip with {compression:?} entries written with Data Descriptions should not fail");
            }
       }

        validate_zip_central_dir(
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
async fn test_validate_invalid_manifest_fails() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let mut rng = fixtures::seeded_rng(function_name!());

    let args = fixtures::CheckZipArgs {
        compression: Compression::Deflate,
        data_descriptors: true,
        ..fixtures::CheckZipArgs::seeded_args(&mut rng, 10, None)
    };

    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .expect("Error creating and validating with {compression:?}");

    let bytes_result = validate_zip_entry_bytes(
        &s3_client,
        args.manifest_file.as_ref().unwrap(),
        &args.dst_obj,
    )
    .await;

    assert!(
        bytes_result.is_ok(),
        "Streaming read of zip entries written with Data Descriptions should not fail"
    );

    let manifest_file = &args.manifest_file.as_ref().unwrap();
    let mut writer =
        cobalt_aws::s3::AsyncPutObject::new(&s3_client, &manifest_file.bucket, &manifest_file.key);
    let bytes = fixtures::fetch_bytes(&s3_client, manifest_file)
        .await
        .unwrap();
    for line in std::io::BufReader::new(&bytes[..]).lines() {
        let entry = serde_json::from_str::<ManifestEntry>(&line.unwrap()).unwrap();
        let entry = ManifestEntry {
            crc32: rng.gen(),
            ..entry
        };
        writer
            .write_all(&serde_json::json!(entry).to_string().into_bytes())
            .await
            .unwrap();
    }
    writer.close().await.unwrap();

    let bytes_result = validate_zip_entry_bytes(
        &s3_client,
        args.manifest_file.as_ref().unwrap(),
        &args.dst_obj,
    )
    .await;
    assert!(
        bytes_result.is_err(),
        "Invalid manifest should fail validation"
    );
}

#[tokio::test]
#[named]
async fn test_validate_incomplete_manifest_fails() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let mut rng = fixtures::seeded_rng(function_name!());

    let args = fixtures::CheckZipArgs {
        compression: Compression::Deflate,
        data_descriptors: true,
        ..fixtures::CheckZipArgs::seeded_args(&mut rng, 10, None)
    };

    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .expect("Error creating and validating with {compression:?}");

    let bytes_result = validate_zip_entry_bytes(
        &s3_client,
        args.manifest_file.as_ref().unwrap(),
        &args.dst_obj,
    )
    .await;

    assert!(
        bytes_result.is_ok(),
        "Streaming read of zip entries written with Data Descriptions should not fail"
    );

    let manifest_file = &args.manifest_file.as_ref().unwrap();
    let mut writer =
        cobalt_aws::s3::AsyncPutObject::new(&s3_client, &manifest_file.bucket, &manifest_file.key);
    let bytes = fixtures::fetch_bytes(&s3_client, manifest_file)
        .await
        .unwrap();
    for line in std::io::BufReader::new(&bytes[..]).lines().skip(1) {
        let entry = serde_json::from_str::<ManifestEntry>(&line.unwrap()).unwrap();
        writer
            .write_all(&serde_json::json!(entry).to_string().into_bytes())
            .await
            .unwrap();
    }
    writer.close().await.unwrap();

    let bytes_result = validate_zip_entry_bytes(
        &s3_client,
        args.manifest_file.as_ref().unwrap(),
        &args.dst_obj,
    )
    .await;
    assert!(
        bytes_result.is_err(),
        "Incomplete manifest should fail validation"
    );
}

#[tokio::test]
#[named]
async fn test_validate_manifest() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let mut rng = fixtures::seeded_rng(function_name!());

    let bucket = "test-bucket";
    fixtures::create_bucket(&s3_client, bucket).await.unwrap();

    //create a random file
    let object = S3Object::new(bucket, fixtures::gen_random_file_name(&mut rng));
    fixtures::create_random_file(&s3_client, &object, bytesize::KB as usize)
        .await
        .unwrap();
    let crc32 = fixtures::object_crc32(&s3_client, &object).await.unwrap();

    let manifest_file = S3Object::new(bucket, fixtures::gen_random_file_name(&mut rng));
    let mut writer =
        cobalt_aws::s3::AsyncPutObject::new(&s3_client, &manifest_file.bucket, &manifest_file.key);
    let entry = ManifestEntry {
        object: object.clone(),
        filename_in_zip: "".into(),
        crc32,
    };
    writer
        .write_all(&serde_json::json!(entry).to_string().into_bytes())
        .await
        .unwrap();
    writer.close().await.unwrap();

    assert!(validate_manifest_file(&s3_client, &manifest_file, 1, 1)
        .await
        .is_ok());
}

#[tokio::test]
#[named]
async fn test_validate_manifest_invalid_crc() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let mut rng = fixtures::seeded_rng(function_name!());

    let bucket = "test-bucket";
    fixtures::create_bucket(&s3_client, bucket).await.unwrap();

    //create a random file
    let object = S3Object::new(bucket, fixtures::gen_random_file_name(&mut rng));
    fixtures::create_random_file(&s3_client, &object, bytesize::KB as usize)
        .await
        .unwrap();
    let crc32 = fixtures::object_crc32(&s3_client, &object).await.unwrap();

    let manifest_file = S3Object::new(bucket, fixtures::gen_random_file_name(&mut rng));
    let mut writer =
        cobalt_aws::s3::AsyncPutObject::new(&s3_client, &manifest_file.bucket, &manifest_file.key);
    let entry = ManifestEntry {
        object: object.clone(),
        filename_in_zip: "".into(),
        crc32: crc32 + 100,
    };
    writer
        .write_all(&serde_json::json!(entry).to_string().into_bytes())
        .await
        .unwrap();
    writer.close().await.unwrap();

    assert!(validate_manifest_file(&s3_client, &manifest_file, 1, 1)
        .await
        .is_err());
}

#[tokio::test]
#[named]
async fn test_unarchive_invalid_dst() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let test_bucket = "test-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());
    let src_key = fixtures::gen_random_file_name(&mut rng);
    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let src = S3Object::new(test_bucket, &src_key);
    fixtures::create_bucket(&s3_client, test_bucket)
        .await
        .unwrap();
    fixtures::create_random_file(&s3_client, &src, 10)
        .await
        .unwrap();

    let dst: S3Object = S3Object::new(test_bucket, &dst_key);
    let archiver = Archiver::builder().compression(Compression::Stored).build();
    archiver
        .create_zip(&s3_client, vec![Ok(src)].into_iter(), &dst, None)
        .await
        .expect("Expected zip creation");

    let unarchive_prefix = fixtures::gen_random_file_name(&mut rng);
    let unarchive_dst = S3Object::new(test_bucket, &unarchive_prefix);
    assert!(
        cobalt_s3_archiver::unarchive_all(&s3_client, &dst, &unarchive_dst, 5 * MIB as usize)
            .await
            .is_err()
    );
}

#[tokio::test]
#[named]
async fn test_unarchive() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let test_bucket = "test-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());
    let src_key = fixtures::gen_random_file_name(&mut rng);
    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let src = S3Object::new(test_bucket, &src_key);
    fixtures::create_bucket(&s3_client, test_bucket)
        .await
        .unwrap();
    fixtures::create_random_file(&s3_client, &src, 10)
        .await
        .unwrap();

    let dst: S3Object = S3Object::new(test_bucket, &dst_key);
    let archiver = Archiver::builder().compression(Compression::Stored).build();
    archiver
        .create_zip(&s3_client, vec![Ok(src)].into_iter(), &dst, None)
        .await
        .expect("Expected zip creation");

    let unarchive_prefix = fixtures::gen_random_file_name(&mut rng) + "/";
    let unarchive_dst = S3Object::new(test_bucket, &unarchive_prefix);
    cobalt_s3_archiver::unarchive_all(&s3_client, &dst, &unarchive_dst, 5 * MIB as usize)
        .await
        .unwrap();

    let extracted_key = S3Object::new(test_bucket, unarchive_prefix + &src_key);
    assert!(fixtures::check_object_exists(&s3_client, &extracted_key)
        .await
        .unwrap());
}

#[tokio::test]
#[named]
async fn test_check_unarchive_with_dirs() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let mut rng = fixtures::seeded_rng(function_name!());
    let args = fixtures::CheckZipArgs::seeded_args(&mut rng, 10, Some(&["dir_one", "dir_two"]));
    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .unwrap();
    let unarchive_prefix = fixtures::gen_random_file_name(&mut rng) + "/";
    let unarchive_dst = S3Object::new(args.src_bucket, &unarchive_prefix);
    cobalt_s3_archiver::unarchive_all(&s3_client, &args.dst_obj, &unarchive_dst, 5 * MIB as usize)
        .await
        .unwrap();
    for obj in args.src_keys {
        let extracted_obj =
            S3Object::new(&unarchive_dst.bucket, unarchive_prefix.to_owned() + &obj);
        assert!(fixtures::check_object_exists(&s3_client, &extracted_obj)
            .await
            .unwrap());
    }
}

#[tokio::test]
#[named]
async fn test_list_archive() {
    let test_client = S3TestClient::default();
    let (_container, s3_client) = test_client.client().await;

    let mut rng = fixtures::seeded_rng(function_name!());
    let args = fixtures::CheckZipArgs::seeded_args(&mut rng, 10, Some(&["dir_one", "dir_two"]));
    fixtures::create_and_validate_zip(&s3_client, &args)
        .await
        .unwrap();
    let entries = cobalt_s3_archiver::ZipEntries::new(&s3_client, &args.dst_obj)
        .await
        .unwrap();
    for (obj, entry) in args.src_keys.iter().zip(entries.into_iter()) {
        assert_eq!(obj, entry.filename())
    }
}
