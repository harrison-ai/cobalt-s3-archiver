pub mod common;

use std::io::SeekFrom;

use ::function_name::named;
use bytesize::MIB;
use common::aws::S3TestClient;
use common::fixtures;
use futures::prelude::*;
use s3_archiver::aws::{AsyncMultipartUpload, S3ObjectSeekableRead};
use s3_archiver::S3Object;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
#[cfg(feature = "test_containers")]
use {futures::lock::Mutex, futures::stream, std::sync::Arc};

#[tokio::test]
#[named]
async fn test_put_single_part() {
    let test_client = S3TestClient::default();
    let (_container, client) = test_client.client().await;
    let test_bucket = "test-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());
    let dst_key = fixtures::gen_random_file_name(&mut rng);

    fixtures::create_bucket(&client, test_bucket).await.unwrap();
    let buffer_len = MIB as usize;

    let mut upload =
        AsyncMultipartUpload::new(&client, test_bucket, &dst_key, 5_usize * MIB as usize, None)
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
    let data_len = 10 * MIB as usize;

    let mut upload =
        AsyncMultipartUpload::new(&client, test_bucket, &dst_key, 5 * MIB as usize, None)
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
        AsyncMultipartUpload::new(&client, test_bucket, &dst_key, 5 * MIB as usize, None)
            .await
            .unwrap();

    let data_len = 14 * MIB as usize;

    upload.write_all(&vec![0; data_len]).await.unwrap();
    upload.close().await.unwrap();
}

#[tokio::test]
#[named]
async fn test_put_16mb_single_upload() {
    let test_client = S3TestClient::default();
    let (_container, client) = test_client.client().await;
    let test_bucket = "test-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());
    let dst_key = fixtures::gen_random_file_name(&mut rng);

    fixtures::create_bucket(&client, test_bucket).await.unwrap();

    let mut upload =
        AsyncMultipartUpload::new(&client, test_bucket, &dst_key, 5 * MIB as usize, Some(1))
            .await
            .unwrap();

    let data_len = 16 * MIB as usize;

    upload.write_all(&vec![0; data_len]).await.unwrap();
    upload.close().await.unwrap();
    let bytes = fixtures::fetch_bytes(&client, &S3Object::new(test_bucket, &dst_key))
        .await
        .unwrap();
    assert_eq!(data_len, bytes.len())
}

#[tokio::test]
#[named]
#[cfg(feature = "test_containers")]
async fn test_fail_failed_write() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    let test_bucket = "test-bucket";

    let mut rng = fixtures::seeded_rng(function_name!());
    let dst_key = fixtures::gen_random_file_name(&mut rng);

    fixtures::create_bucket(&client, test_bucket).await.unwrap();

    let part_len = 5 * MIB as usize;
    let upload = Arc::new(Mutex::new(
        AsyncMultipartUpload::new(&client, test_bucket, &dst_key, part_len, None)
            .await
            .unwrap(),
    ));

    //Create a file big enough to upload a part
    let data = vec![0; part_len];
    upload.lock().await.write_all(&data).await.unwrap();
    //Drop the container
    drop(container);

    while client.list_buckets().send().await.is_ok() {}

    //generate some more writes and a poll should fail
    let result: Result<(), _> = stream::iter(0..500)
        .then(|_| {
            let upload = upload.clone();
            let data = vec![0; MIB as usize];
            async move { upload.lock().await.write_all(&data).await }
        })
        .map_ok(|_| ())
        .try_collect()
        .await;
    assert!(result.is_err());
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
        AsyncMultipartUpload::new(&client, test_bucket, &dst_key, 5 * MIB as usize, None)
            .await
            .unwrap();

    let data_len = 6 * MIB as usize;

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
        AsyncMultipartUpload::new(&client, test_bucket, &dst_key, 5 * MIB as usize, None)
            .await
            .unwrap();

    let data_len = 6 * MIB as usize;

    drop(container);
    upload.write_all(&vec![0; data_len]).await.unwrap();
    assert!(upload.close().await.is_err());
}

#[tokio::test]
#[named]
async fn test_s3objectseekableread() {
    let test_client = S3TestClient::default();
    let (_container, client) = test_client.client().await;
    let test_bucket = "test-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());
    let dst_key = fixtures::gen_random_file_name(&mut rng);

    fixtures::create_bucket(&client, test_bucket).await.unwrap();

    let mut upload =
        AsyncMultipartUpload::new(&client, test_bucket, &dst_key, 5 * MIB as usize, None)
            .await
            .unwrap();

    let data_len = 6 * MIB as usize;
    upload.write_all(&vec![0; data_len]).await.unwrap();
    upload.close().await.unwrap();

    let dst_obj = S3Object::new(test_bucket, &dst_key);
    let mut read = S3ObjectSeekableRead::new(&client, &dst_obj, None)
        .await
        .unwrap();

    let mut buff = std::io::Cursor::new(vec![]);
    let count_read = tokio::io::copy(&mut read, &mut buff).await.unwrap();

    assert_eq!(data_len, count_read as usize);
}

#[tokio::test]
#[named]
async fn test_s3objectseekableread_seek() {
    let test_client = S3TestClient::default();
    let (_container, client) = test_client.client().await;
    let test_bucket = "test-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());
    let dst_key = fixtures::gen_random_file_name(&mut rng);

    fixtures::create_bucket(&client, test_bucket).await.unwrap();

    let mut upload =
        AsyncMultipartUpload::new(&client, test_bucket, &dst_key, 5 * MIB as usize, None)
            .await
            .unwrap();

    let data_len = 6 * MIB as usize;
    upload.write_all(&vec![0; data_len]).await.unwrap();
    upload.close().await.unwrap();

    let dst_obj = S3Object::new(test_bucket, &dst_key);
    let mut read = S3ObjectSeekableRead::new(&client, &dst_obj, None)
        .await
        .unwrap();

    read.seek(SeekFrom::Start(MIB)).await.unwrap();

    let mut buff = std::io::Cursor::new(vec![]);
    let count_read = tokio::io::copy(&mut read, &mut buff).await.unwrap();

    assert_eq!(data_len - MIB as usize, count_read as usize);
}

#[tokio::test]
#[named]
async fn test_s3objectseekableread_seek_jump() {
    let test_client = S3TestClient::default();
    let (_container, client) = test_client.client().await;
    let test_bucket = "test-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());
    let dst_key = fixtures::gen_random_file_name(&mut rng);

    fixtures::create_bucket(&client, test_bucket).await.unwrap();

    let mut upload =
        AsyncMultipartUpload::new(&client, test_bucket, &dst_key, 5 * MIB as usize, None)
            .await
            .unwrap();

    let data_len = 6 * MIB as usize;
    upload.write_all(&vec![0; data_len]).await.unwrap();
    upload.close().await.unwrap();

    let dst_obj = S3Object::new(test_bucket, &dst_key);
    let mut read = S3ObjectSeekableRead::new(&client, &dst_obj, None)
        .await
        .unwrap();

    read.seek(SeekFrom::Start(MIB)).await.unwrap();
    //Arrays are allocated on the stack
    let mut tmp_buf = Box::new([1_u8; MIB as usize]);
    read.read_exact(tmp_buf.as_mut()).await.unwrap();

    read.seek(SeekFrom::End(-i64::try_from(MIB).unwrap()))
        .await
        .unwrap();

    let mut buff = std::io::Cursor::new(vec![]);
    let count_read = tokio::io::copy(&mut read, &mut buff).await.unwrap();

    assert_eq!(MIB as usize, count_read as usize);
}
