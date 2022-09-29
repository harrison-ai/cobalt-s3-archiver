pub mod common;

use assert_cmd::Command;
use common::aws::S3TestClient;
use common::fixtures;
use std::ffi::OsStr;
use testcontainers::{Container, Image};

fn set_env_if_not_set<K: AsRef<OsStr>, V: AsRef<OsStr>>(key: K, value: V) {
    if std::env::var(&key).is_err() {
        std::env::set_var(&key, &value);
    }
}

fn set_aws_env<T: Image>(container: &Option<Box<Container<T>>>) {
    // cobalt-aws magic picks up these env vars and sets the endpoint
    if let Some(ref cont) = container {
        let port = cont.get_host_port_ipv4(4566);
        std::env::set_var("EDGE_PORT", port.to_string());
    }
    set_env_if_not_set("LOCALSTACK_HOSTNAME", common::aws::localstack_host());
    set_env_if_not_set("AWS_DEFAULT_REGION", "ap-southeast-2");
    set_env_if_not_set("AWS_ACCESS_KEY_ID", "test");
    set_env_if_not_set("AWS_SECRET_ACCESS_KEY", "test");
}

#[tokio::test]
async fn test_cli_run() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    set_aws_env(&container);

    let keys = ["key_1.txt", "key_2.txt"];
    let src_bucket = "src-bucket";
    let dst_bucket = "dst-bucket";
    fixtures::create_bucket(&client, src_bucket).await.unwrap();
    fixtures::create_bucket(&client, dst_bucket).await.unwrap();
    let src_objects: Vec<_> = fixtures::s3_object_from_keys(src_bucket, keys).collect();
    fixtures::create_random_files(&client, 1024_usize.pow(2), &src_objects)
        .await
        .unwrap();

    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg(format!("s3://{dst_bucket}/output.zip"));
    for key in &keys {
        cmd.write_stdin(format!("s3://{src_bucket}/{key}"));
    }
    cmd.assert().success();
}
