pub mod aws;

use anyhow::{Context, Result};
use async_zip::{write::EntryOptions, Compression as AsyncCompression};
use aws::AsyncMultipartUpload;
use clap::ValueEnum;
use tokio::io::AsyncWriteExt;
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use tokio_util::io::StreamReader;
use url::Url;

/// A bucket key pair for a S3Object
pub struct S3Object {
    /// The bucket the object is in.
    pub bucket: String,
    /// The key the in the bucket for the object.
    pub key: String,
}

impl S3Object {
    pub fn new(bucket: &str, key: &str) -> Self {
        S3Object {
            bucket: bucket.to_owned(),
            key: key.trim_start_matches('/').to_owned(),
        }
    }
}

/// Convert from an Url into a S3Object
impl TryFrom<url::Url> for S3Object {
    type Error = anyhow::Error;

    fn try_from(value: url::Url) -> Result<Self, Self::Error> {
        if value.scheme() != "s3" {
            anyhow::bail!("S3 URL must have a scheme of s3")
        }
        let bucket = value.host_str().context("S3 URL must have host")?;
        let key = value.path();

        if key.is_empty() {
            anyhow::bail!("S3 URL must have a path")
        }
        Ok(S3Object::new(bucket, key))
    }
}

impl TryFrom<&str> for S3Object {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse::<Url>()?.try_into()
    }
}

impl TryFrom<String> for S3Object {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse::<Url>()?.try_into()
    }
}

#[derive(Debug, Clone, ValueEnum, Copy, PartialEq, Eq)]
pub enum Compression {
    Stored,
    Deflate,
    Bzip,
    Lzma,
    Zstd,
    Xz,
}

impl From<Compression> for AsyncCompression {
    fn from(c: Compression) -> Self {
        match c {
            Compression::Stored => AsyncCompression::Stored,
            Compression::Deflate => AsyncCompression::Deflate,
            Compression::Bzip => AsyncCompression::Bz,
            Compression::Lzma => AsyncCompression::Lzma,
            Compression::Zstd => AsyncCompression::Zstd,
            Compression::Xz => AsyncCompression::Xz,
        }
    }
}

pub async fn create_zip<'a, I>(
    client: &aws_sdk_s3::Client,
    srcs: I,
    prefix_strip: Option<&'a str>,
    compression: Compression,
    dst: &S3Object,
) -> Result<()>
where
    I: IntoIterator<Item = Result<S3Object>>,
{
    let mut upload =
        AsyncMultipartUpload::new(client, &dst.bucket, &dst.key, 5_usize * 1024_usize.pow(2))
            .await?
            .compat_write();
    let mut zip = async_zip::write::ZipFileWriter::new(&mut upload);
    //TODO Turn this into a stream so the objects can be fetched async
    for src in srcs {
        let src = src?;
        let response = client
            .get_object()
            .bucket(&src.bucket)
            .key(&src.key)
            .send()
            .await?;
        let opts = EntryOptions::new(
            src.key
                .trim_start_matches(prefix_strip.unwrap_or_default())
                .into(),
            compression.into(),
        );
        let mut entry_writer = zip.write_entry_stream(opts).await?;
        let mut read = StreamReader::new(response.body);
        let _ = tokio::io::copy(&mut read, &mut entry_writer).await?;
        entry_writer.close().await?;
    }
    zip.close().await?;
    upload.shutdown().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_tryfrom() {
        let bucket = "test-bucket.to_owned()";
        let key = "test-key";
        let url: url::Url = format!("s3://{bucket}/{key}")
            .parse()
            .expect("Expected successful URL parsing");
        let obj: S3Object = url.try_into().expect("Expected successful URL conversion");
        assert_eq!(bucket, obj.bucket);
        assert_eq!(key, obj.key);
    }

    #[test]
    fn test_s3_tryfrom_no_path() {
        let url: url::Url = "s3://test-bucket"
            .parse()
            .expect("Expected successful URL parsing");
        let result: Result<S3Object> = url.try_into();
        assert!(result.is_err())
    }

    #[test]
    fn test_s3_tryfrom_file_url() {
        let url: url::Url = "file://path/to/file"
            .parse()
            .expect("Expected successful URL parsing");
        let result: Result<S3Object> = url.try_into();
        assert!(result.is_err())
    }
}
