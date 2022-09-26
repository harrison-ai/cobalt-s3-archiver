mod common;

use common::aws::S3TestClient;
use common::fixtures;
use futures::prelude::*;
use s3_archiver::aws::AsyncMultipartUpload;
#[cfg(feature = "test_containers")]
use testcontainers::clients;

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

    let mut upload = AsyncMultipartUpload::new(&client, test_bucket, dst_key)
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
