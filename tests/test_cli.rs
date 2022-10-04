pub mod common;

use ::function_name::named;
use assert_cmd::Command;
use common::aws::S3TestClient;
use common::fixtures;
use std::ffi::OsStr;
use std::io::{self, Write};
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
#[named]
async fn test_cli_run() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    set_aws_env(&container);
    let src_bucket = "src-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());

    let src_objs =
        fixtures::s3_object_from_keys(src_bucket, fixtures::gen_random_file_names(&mut rng, 2))
            .collect();
    fixtures::create_src_files(&client, src_bucket, &src_objs, 1024_usize.pow(2))
        .await
        .unwrap();

    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let dst_obj = s3_archiver::S3Object::new("dst-bucket", &dst_key);
    fixtures::create_bucket(&client, &dst_obj.bucket)
        .await
        .unwrap();
    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg(format!("s3://{}/{}", dst_obj.bucket, dst_obj.key));
    cmd.write_stdin(
        src_objs
            .iter()
            .map(|obj| format!("s3://{}/{}", obj.bucket, obj.key))
            .collect::<Vec<_>>()
            .join("\n"),
    );
    let output = cmd.output().unwrap();
    io::stdout().write_all(&output.stdout).unwrap();
    io::stderr().write_all(&output.stderr).unwrap();

    assert!(output.status.success());
    fixtures::validate_zip(&client, &dst_obj, None, src_objs.iter())
        .await
        .unwrap()
}

#[test]
fn test_cli_no_args() {
    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.assert().failure();
}

#[test]
fn test_cli_invalid_dst_s3_url() {
    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("not_valid_s3_url");
    cmd.assert().failure();
}

#[tokio::test]
#[named]
async fn test_cli_no_trailing_slash_prefix() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    set_aws_env(&container);
    let mut rng = fixtures::seeded_rng(function_name!());

    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let dst_obj = s3_archiver::S3Object::new("dst-bucket", &dst_key);
    fixtures::create_bucket(&client, &dst_obj.bucket)
        .await
        .unwrap();
    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("-p").arg("no_trailing_slash");
    cmd.arg(format!("s3://{}/{}", dst_obj.bucket, dst_obj.key));
    cmd.assert().failure();
}

#[tokio::test]
#[named]
async fn test_cli_has_leading_slash_prefix() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    set_aws_env(&container);
    let mut rng = fixtures::seeded_rng(function_name!());

    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let dst_obj = s3_archiver::S3Object::new("dst-bucket", &dst_key);
    fixtures::create_bucket(&client, &dst_obj.bucket)
        .await
        .unwrap();
    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("-p").arg("/no_trailing_slash/");
    cmd.arg(format!("s3://{}/{}", dst_obj.bucket, dst_obj.key));
    cmd.assert().failure();
}

#[tokio::test]
#[named]
async fn test_invalid_s3_src() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    set_aws_env(&container);
    let mut rng = fixtures::seeded_rng(function_name!());

    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let dst_obj = s3_archiver::S3Object::new("dst-bucket", &dst_key);
    fixtures::create_bucket(&client, &dst_obj.bucket)
        .await
        .unwrap();

    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg(format!("s3://{}/{}", dst_obj.bucket, dst_obj.key));
    cmd.write_stdin("an_invalid_src_url");
    cmd.assert().failure();
}
