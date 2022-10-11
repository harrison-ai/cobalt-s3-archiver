pub mod aws;
pub mod counter;

use std::str::FromStr;

use crate::counter::ByteLimit;
use anyhow::{bail, ensure, Context, Result};
use async_zip::{Compression as AsyncCompression, ZipEntryBuilder};
use aws::AsyncMultipartUpload;
use clap::ValueEnum;
use tokio::io::AsyncWriteExt;
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use tokio_util::io::StreamReader;
use url::Url;

/// A bucket key pair for a S3Object
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct S3Object {
    /// The bucket the object is in.
    pub bucket: String,
    /// The key the in the bucket for the object.
    pub key: String,
}

impl S3Object {
    pub fn new(bucket: impl AsRef<str>, key: impl AsRef<str>) -> Self {
        S3Object {
            bucket: bucket.as_ref().to_owned(),
            key: key.as_ref().trim_start_matches('/').to_owned(),
        }
    }
}

/// Convert from an Url into a S3Object
impl TryFrom<url::Url> for S3Object {
    type Error = anyhow::Error;

    fn try_from(value: url::Url) -> Result<Self, Self::Error> {
        if value.scheme() != "s3" {
            bail!("S3 URL must have a scheme of s3")
        }
        let bucket = value.host_str().context("S3 URL must have host")?;
        let key = value.path();

        if key.is_empty() {
            bail!("S3 URL must have a path")
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

impl FromStr for S3Object {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
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

const MAX_FILES_IN_ZIP: u16 = u16::MAX;
const MAX_FILE_IN_ZIP_SIZE_BYTES: u32 = u32::MAX;
const MAX_ZIP_FILE_SIZE_BYTES: u32 = u32::MAX;

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
    //S3 keys can start with "/" but this is a little confusing
    ensure!(
        prefix_strip.filter(|s| s.starts_with('/')).is_none(),
        "prefix_strip must not start with `/`"
    );
    //Fail early to ensure that the dir structure in the Zip does not have '/' at the root.
    ensure!(
        prefix_strip.is_none() || prefix_strip.filter(|s| s.ends_with('/')).is_some(),
        "prefix_strip must end with `/`"
    );

    println!("Creating zip file from dst_key {:?}", dst);
    //Create the upload and the zip writer.
    let upload = AsyncMultipartUpload::new(
        client,
        &dst.bucket,
        &dst.key,
        5_usize * 1024_usize.pow(2),
        None,
    )
    .await?;

    let mut byte_limit =
        ByteLimit::new_from_inner(upload, MAX_ZIP_FILE_SIZE_BYTES.into()).compat_write();
    let mut zip = async_zip::write::ZipFileWriter::new(&mut byte_limit);

    //Copy each src object into the zip correcting the path based on the `prefix_strip`
    for (src, i) in srcs.into_iter().zip(0_u32..) {
        ensure!(
            i <= MAX_FILES_IN_ZIP.into(),
            "ZIP64 is not supported: Too many zip entries."
        );

        let src = src?;
        // Entry_path is
        let entry_path = src.key.trim_start_matches(prefix_strip.unwrap_or_default());
        ensure!(
            !entry_path.is_empty(),
            "{} with out prefix {prefix_strip:?} is an invalid entry ",
            src.key
        );
        println!("Adding file with {entry_path} from src_key {}", src.key);
        let response = client
            .get_object()
            .bucket(&src.bucket)
            .key(&src.key)
            .send()
            .await?;
        ensure!(response.content_length() <= MAX_FILE_IN_ZIP_SIZE_BYTES.into(),
            "ZIP64 is not supported: Max file size is {MAX_FILE_IN_ZIP_SIZE_BYTES}, {src:?} is {} bytes", response.content_length);
        let opts = ZipEntryBuilder::new(entry_path.to_owned(), compression.into());
        let mut entry_writer = zip.write_entry_stream(opts).await?;
        let mut read = StreamReader::new(response.body);
        let _ = tokio::io::copy(&mut read, &mut entry_writer).await?;
        // If this is not done the Zip file produced sitently corrupts
        entry_writer.close().await?;
    }
    zip.close().await?;
    //The zip writer does not close the multipart upload
    byte_limit.shutdown().await?;
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
