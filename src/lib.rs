pub mod aws;
pub mod counter;

use std::str::FromStr;

use crate::counter::ByteLimit;
use anyhow::{bail, ensure, Context, Result};
use async_zip::{Compression as AsyncCompression, ZipEntryBuilder};
use aws::AsyncMultipartUpload;
use clap::ValueEnum;
use futures::prelude::*;
use futures::stream;
use tokio::io::AsyncWriteExt;
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use url::Url;
use serde::{Serialize, Deserialize};

/// A bucket key pair for a S3Object
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestEntry {
    object: S3Object,
    crc32: u32
}

impl ManifestEntry {

    pub fn new(object: S3Object, crc32: u32) -> Self {
        ManifestEntry{
            object,
            crc32
        }
    }
}

pub async fn create_zip<'a, I>(
    client: &aws_sdk_s3::Client,
    srcs: I,
    prefix_strip: Option<&'a str>,
    compression: Compression,
    part_size: usize,
    src_fetch_buffer: usize,
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
    let upload = AsyncMultipartUpload::new(client, &dst.bucket, &dst.key, part_size, None).await?;
    //TODO: Pass in manifest S3Object in an optional.
    let mut manifest_upload = cobalt_aws::s3::AsyncPutObject::new(client, &dst.bucket, &(dst.key.to_owned() + ".manifest.jsonl"));

    let mut byte_limit =
        ByteLimit::new_from_inner(upload, MAX_ZIP_FILE_SIZE_BYTES.into()).compat_write();
    let mut zip = async_zip::write::ZipFileWriter::new(&mut byte_limit);


    stream::iter(srcs.into_iter().zip(0_u32..)).map(|(src, i)|{
        ensure!(
            i <= MAX_FILES_IN_ZIP.into(),
            "ZIP64 is not supported: Too many zip entries."
        );
        let src = src?;
        // Entry_path is
        let entry_path = src.key.trim_start_matches(prefix_strip.unwrap_or_default()).to_owned();
        ensure!(
            !entry_path.is_empty(),
            "{} with out prefix {prefix_strip:?} is an invalid entry ",
            src.key
        );
       Ok((src, entry_path))
    }).map_ok(move |(src, entry_path)|{
        client
            .get_object()
            .bucket(&src.bucket)
            .key(&src.key)
            .send()
            .map_ok(|r|(r, src, entry_path))
            .map_err(anyhow::Error::from)
    })
    .try_buffered(src_fetch_buffer)
    .try_fold((&mut zip, &mut manifest_upload), | (zip, manifest_upload), (mut response, src, entry_path)|{
        async move {
            ensure!(response.content_length() <= MAX_FILE_IN_ZIP_SIZE_BYTES.into(),
                "ZIP64 is not supported: Max file size is {MAX_FILE_IN_ZIP_SIZE_BYTES}, {src:?} is {} bytes", response.content_length);
            let opts = ZipEntryBuilder::new(entry_path.to_owned(), compression.into());
            let entry_writer = zip.write_entry_stream(opts).await?;
            use tokio_util::compat::TokioAsyncWriteCompatExt;
            // Use a sink here in order to fan out
            let mut writer_sink = TokioAsyncWriteCompatExt::compat_write(entry_writer).into_sink();

            //It was hard to get this into the Sink because of the lifetimes
            let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
            let mut crc_sink = crate::counter::CRC32Sink::new(&crc32);

            while let Some(data) = response.body.try_next().await? {
                writer_sink.send(data.clone()).await?;
                crc_sink.send(data.clone()).await?;
            }
            // If this is not done the Zip file produced silently corrupts
            writer_sink.close().await?;
            crc_sink.close().await?;
            let manifest_entry = ManifestEntry::new(src, crc_sink.value().context("Expected a CRC32")?);
            let manifest_entry = serde_json::to_string(&manifest_entry)? + "\n";
            manifest_upload.write_all(manifest_entry.as_bytes()).await?;
            Ok((zip, manifest_upload))
        }}).await?;

    zip.close().await?;
    ////The zip writer does not close the multipart upload
    byte_limit.shutdown().await?;
    manifest_upload.close().await?;
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
