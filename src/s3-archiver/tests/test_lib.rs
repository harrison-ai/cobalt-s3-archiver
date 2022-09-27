mod common;

#[cfg(feature = "test_containers")]
use testcontainers::clients;

use common::aws::S3TestClient;
use common::fixtures;
use s3_archiver::{Compression, S3Object};

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

    fixtures::create_bucket(&s3_client, test_bucket).await.unwrap();
    fixtures::create_random_file(&s3_client, &S3Object::new(test_bucket, src_key), 10)
        .await
        .unwrap();

    let src = S3Object::new(test_bucket, src_key);
    let dst: S3Object = S3Object::new(test_bucket, dst_key);
    s3_archiver::create_zip(
        &s3_client,
        vec![Ok(src)].into_iter(),
        "",
        Compression::Stored,
        &dst,
    )
    .await
    .expect("Expected zip creation");

    assert!(fixtures::check_object_exists(&s3_client, &dst).await.unwrap());

}

#[tokio::test]
async fn test_check_zip() {
    #[cfg(feature = "test_containers")]
    let test_client = S3TestClient::TestContainer(clients::Cli::default());
    #[cfg(not(feature = "test_containers"))]
    let test_client = S3TestClient::DockerCompose;

    let (_container, s3_client) = test_client.client().await;

    let test_bucket = "test-bucket";
    let dst_key = "dst_check_file.zip";

    let src_files = ["src-file_1.txt", "src-file_2.txt"];
    common::fixtures::create_bucket(&s3_client, test_bucket)
        .await
        .unwrap();
    for key in src_files {
        fixtures::create_random_file(
            &s3_client,
            &S3Object::new(test_bucket, key),
            1024_usize.pow(2),
        )
        .await
        .unwrap();
    }
    let src_objs = src_files
        .into_iter()
        .map(|key| Ok(S3Object::new(test_bucket, key)));
    let dst: S3Object = S3Object::new(test_bucket, dst_key);
    s3_archiver::create_zip(&s3_client, src_objs, "", Compression::Stored, &dst)
        .await
        .expect("Expected zip creation");

    let bytes = fixtures::fetch_bytes(&s3_client, &dst).await.unwrap();

    //It was not possible to get this to work using the streaming ZipFileReader
    //This issue show similar issues https://github.com/Majored/rs-async-zip/issues/29
    use async_zip::read::mem::ZipFileReader;
    let zip = ZipFileReader::new(&bytes).await.unwrap();
    for entry in zip.entries() {
        assert!(entry.name().starts_with("src"))
    }
}
