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
    let dst_obj = s3_archiver::S3Object::new("dst-bucket", "output.zip");
    let src_objs = fixtures::s3_object_from_keys(src_bucket, &keys).collect();
    fixtures::create_src_files(&client, src_bucket, &src_objs, 1024_usize.pow(2))
        .await
        .unwrap();
    fixtures::create_bucket(&client, &dst_obj.bucket)
        .await
        .unwrap();
    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg(format!("s3://{}/{}", dst_obj.bucket, dst_obj.key));
    cmd.write_stdin(
        keys.map(|key| format!("s3://{src_bucket}/{key}"))
            .join("\n"),
    );
    //cmd.assert().success();
    use std::io::{self, Write};
    let output = cmd.output().unwrap();
    io::stdout().write_all(&output.stdout).unwrap();
    io::stderr().write_all(&output.stderr).unwrap();

    assert!(output.status.success());
    fixtures::validate_zip(&client, &dst_obj, None, src_objs.iter())
        .await
        .unwrap()
}
