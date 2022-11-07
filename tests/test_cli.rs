pub mod common;

use ::function_name::named;
use assert_cmd::Command;
use bytesize::MIB;
use common::aws::S3TestClient;
use common::fixtures;
use std::collections::HashMap;
use std::io::{self, Write};
use testcontainers::{Container, Image};
use url::Url;

fn get_aws_env<T: Image>(container: &Option<Box<Container<T>>>) -> HashMap<String, String> {
    let mut cmd_env = HashMap::new();
    // cobalt-aws magic picks up these env vars and sets the endpoint
    if let Some(ref cont) = container {
        let port = cont.get_host_port_ipv4(4566);
        cmd_env.insert("EDGE_PORT".into(), port.to_string());
    }
    cmd_env.insert(
        "LOCALSTACK_HOSTNAME".into(),
        std::env::var("LOCALSTACK_HOSTNAME").unwrap_or_else(|_| common::aws::localstack_host()),
    );
    cmd_env.insert(
        "AWS_DEFAULT_REGION".into(),
        std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "ap-southeast-2".into()),
    );
    cmd_env.insert(
        "AWS_ACCESS_KEY_ID".into(),
        std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "test".into()),
    );
    cmd_env.insert(
        "AWS_SECRET_ACCESS_KEY".into(),
        std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "test".into()),
    );
    //Change this to get more debug
    cmd_env.insert("RUST_LOG".into(), "info".into());
    cmd_env
}

#[tokio::test]
#[named]
async fn test_cli_run() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    let env = get_aws_env(&container);
    let src_bucket = "src-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());

    let src_objs =
        fixtures::s3_object_from_keys(src_bucket, fixtures::gen_random_file_names(&mut rng, 2))
            .collect();
    fixtures::create_src_files(&client, src_bucket, &src_objs, MIB as usize)
        .await
        .unwrap();

    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let dst_obj = s3_archiver::S3Object::new("dst-bucket", &dst_key);
    fixtures::create_bucket(&client, &dst_obj.bucket)
        .await
        .unwrap();
    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("archive");
    cmd.arg(Url::try_from(&dst_obj).unwrap().as_str());
    cmd.write_stdin(
        src_objs
            .iter()
            .map(|obj| Url::try_from(obj).map(String::from))
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n"),
    );
    cmd.envs(env);
    let output = cmd.output().unwrap();
    io::stdout().write_all(&output.stdout).unwrap();
    io::stderr().write_all(&output.stderr).unwrap();

    assert!(output.status.success());
    fixtures::validate_zip(&client, &dst_obj, None, src_objs.iter(), None)
        .await
        .unwrap()
}

#[tokio::test]
#[named]
async fn test_cli_run_with_size() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    let env = get_aws_env(&container);
    let src_bucket = "src-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());

    let src_objs =
        fixtures::s3_object_from_keys(src_bucket, fixtures::gen_random_file_names(&mut rng, 2))
            .collect();
    fixtures::create_src_files(&client, src_bucket, &src_objs, MIB as usize)
        .await
        .unwrap();

    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let dst_obj = s3_archiver::S3Object::new("dst-bucket", &dst_key);
    fixtures::create_bucket(&client, &dst_obj.bucket)
        .await
        .unwrap();
    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("archive");
    cmd.arg(Url::try_from(&dst_obj).unwrap().as_str());
    cmd.arg("-s").arg("5MiB");
    cmd.write_stdin(
        src_objs
            .iter()
            .map(|obj| Url::try_from(obj).map(String::from))
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n"),
    );
    cmd.envs(env);
    let output = cmd.output().unwrap();
    io::stdout().write_all(&output.stdout).unwrap();
    io::stderr().write_all(&output.stderr).unwrap();

    assert!(output.status.success());
    fixtures::validate_zip(&client, &dst_obj, None, src_objs.iter(), None)
        .await
        .unwrap()
}

#[tokio::test]
#[named]
async fn test_cli_run_with_src_fetch_buffer() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    let env = get_aws_env(&container);

    let src_bucket = "src-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());

    let src_objs =
        fixtures::s3_object_from_keys(src_bucket, fixtures::gen_random_file_names(&mut rng, 10))
            .collect();
    fixtures::create_src_files(&client, src_bucket, &src_objs, MIB as usize)
        .await
        .unwrap();

    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let dst_obj = s3_archiver::S3Object::new("dst-bucket", &dst_key);
    fixtures::create_bucket(&client, &dst_obj.bucket)
        .await
        .unwrap();
    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("archive");
    cmd.arg(Url::try_from(&dst_obj).unwrap().as_str());
    cmd.arg("-f").arg("10");
    cmd.write_stdin(
        src_objs
            .iter()
            .map(|obj| Url::try_from(obj).map(String::from))
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n"),
    );
    cmd.envs(env);
    let output = cmd.output().unwrap();
    io::stdout().write_all(&output.stdout).unwrap();
    io::stderr().write_all(&output.stderr).unwrap();

    assert!(output.status.success());
    fixtures::validate_zip(&client, &dst_obj, None, src_objs.iter(), None)
        .await
        .unwrap()
}

#[tokio::test]
#[named]
async fn test_cli_run_with_src_manifest() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    let env = get_aws_env(&container);

    let src_bucket = "src-bucket";
    let mut rng = fixtures::seeded_rng(function_name!());

    let src_objs =
        fixtures::s3_object_from_keys(src_bucket, fixtures::gen_random_file_names(&mut rng, 10))
            .collect();
    fixtures::create_src_files(&client, src_bucket, &src_objs, MIB as usize)
        .await
        .unwrap();

    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let dst_obj = s3_archiver::S3Object::new("dst-bucket", &dst_key);
    fixtures::create_bucket(&client, &dst_obj.bucket)
        .await
        .unwrap();
    let manifest_key = fixtures::gen_random_file_name(&mut rng);
    let manifest_obj = s3_archiver::S3Object::new("dst-bucket", &manifest_key);
    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("archive");
    cmd.arg(Url::try_from(&dst_obj).unwrap().as_str());
    cmd.arg("-m")
        .arg(url::Url::try_from(&manifest_obj).unwrap().as_str());
    cmd.write_stdin(
        src_objs
            .iter()
            .map(|obj| Url::try_from(obj).map(String::from))
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n"),
    );
    cmd.envs(env);
    let output = cmd.output().unwrap();
    io::stdout().write_all(&output.stdout).unwrap();
    io::stderr().write_all(&output.stderr).unwrap();

    assert!(output.status.success());
    fixtures::validate_zip(
        &client,
        &dst_obj,
        None,
        src_objs.iter(),
        Some(&manifest_obj),
    )
    .await
    .unwrap()
}

#[test]
fn test_cli_no_args() {
    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("archive");

    cmd.assert().failure();
}

#[test]
fn test_cli_invalid_dst_s3_url() {
    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("archive");
    cmd.arg("not_valid_s3_url");
    cmd.assert().failure();
}

#[tokio::test]
#[named]
async fn test_cli_no_trailing_slash_prefix() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    let env = get_aws_env(&container);
    let mut rng = fixtures::seeded_rng(function_name!());

    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let dst_obj = s3_archiver::S3Object::new("dst-bucket", &dst_key);
    fixtures::create_bucket(&client, &dst_obj.bucket)
        .await
        .unwrap();
    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("archive");
    cmd.arg("-p").arg("no_trailing_slash");
    cmd.arg(Url::try_from(&dst_obj).unwrap().as_str());
    cmd.envs(env);
    cmd.assert().failure();
}

#[tokio::test]
#[named]
async fn test_cli_has_leading_slash_prefix() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    let env = get_aws_env(&container);
    let mut rng = fixtures::seeded_rng(function_name!());

    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let dst_obj = s3_archiver::S3Object::new("dst-bucket", &dst_key);
    fixtures::create_bucket(&client, &dst_obj.bucket)
        .await
        .unwrap();
    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("archive");
    cmd.arg("-p").arg("/no_trailing_slash/");
    cmd.arg(Url::try_from(&dst_obj).unwrap().as_str());
    cmd.envs(env);
    cmd.assert().failure();
}

#[tokio::test]
#[named]
async fn test_invalid_s3_src() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    let env = get_aws_env(&container);
    let mut rng = fixtures::seeded_rng(function_name!());

    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let dst_obj = s3_archiver::S3Object::new("dst-bucket", &dst_key);
    fixtures::create_bucket(&client, &dst_obj.bucket)
        .await
        .unwrap();

    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("archive");
    cmd.arg(Url::try_from(&dst_obj).unwrap().as_str());
    cmd.envs(env);
    cmd.write_stdin("an_invalid_src_url");
    cmd.assert().failure();
}

#[tokio::test]
#[named]
async fn test_invalid_part_size_0b() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    let env = get_aws_env(&container);
    let mut rng = fixtures::seeded_rng(function_name!());

    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let dst_obj = s3_archiver::S3Object::new("dst-bucket", &dst_key);
    fixtures::create_bucket(&client, &dst_obj.bucket)
        .await
        .unwrap();

    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("archive");
    cmd.arg("-s").arg("0");
    cmd.arg(Url::try_from(dst_obj).unwrap().as_str());
    cmd.envs(env);
    cmd.assert().failure();
}

#[tokio::test]
#[named]
async fn test_invalid_part_size_1k() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    let env = get_aws_env(&container);
    let mut rng = fixtures::seeded_rng(function_name!());

    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let dst_obj = s3_archiver::S3Object::new("dst-bucket", &dst_key);
    fixtures::create_bucket(&client, &dst_obj.bucket)
        .await
        .unwrap();

    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("archive");
    cmd.arg("-s").arg("1K");
    cmd.arg(Url::try_from(dst_obj).unwrap().as_str());
    cmd.envs(env);
    cmd.assert().failure();
}

#[tokio::test]
#[named]
async fn test_manifest_file_and_generate() {
    let test_client = S3TestClient::default();
    let (container, client) = test_client.client().await;
    let env = get_aws_env(&container);
    let mut rng = fixtures::seeded_rng(function_name!());

    let dst_key = fixtures::gen_random_file_name(&mut rng);
    let dst_obj = s3_archiver::S3Object::new("dst-bucket", &dst_key);
    let manifest_key = fixtures::gen_random_file_name(&mut rng);
    let manifest_obj = s3_archiver::S3Object::new("dst-bucket", &manifest_key);
    fixtures::create_bucket(&client, &dst_obj.bucket)
        .await
        .unwrap();

    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("archive");
    cmd.arg("-m")
        .arg(Url::try_from(manifest_obj).unwrap().as_str());
    cmd.arg("-g");
    cmd.arg(Url::try_from(dst_obj).unwrap().as_str());
    cmd.envs(env);
    cmd.assert().failure();
}

#[test]
fn test_validate_manifest_file_no_args() {
    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("validate-manifest");
    cmd.assert().failure();
}

#[test]
fn test_validate_manifest_c_with_manifest() {
    let mut cmd = Command::cargo_bin("s3-archiver-cli").unwrap();
    cmd.arg("validate-manifest");
    cmd.arg("-c");
    cmd.arg("s3://test/file");
    cmd.assert().failure();
}
