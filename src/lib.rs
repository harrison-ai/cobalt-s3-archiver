pub mod aws;
pub mod counter;

use std::str::FromStr;
use std::sync::Arc;

use crate::counter::ByteLimit;
use anyhow::{bail, ensure, Context, Result};
use async_zip::{Compression as AsyncCompression, ZipEntryBuilder};
use aws::AsyncMultipartUpload;
use aws_sdk_s3::output::GetObjectOutput;
use clap::ValueEnum;
use futures::lock::Mutex;
use futures::prelude::*;
use futures::stream;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use url::Url;

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
    filename_in_zip: String,
    crc32: u32,
}

impl ManifestEntry {
    pub fn new(object: &S3Object, crc32: u32, filename_in_zip: &str) -> Self {
        ManifestEntry {
            object: object.clone(),
            crc32,
            filename_in_zip: filename_in_zip.to_owned(),
        }
    }
}

pub struct ManifestFileUpload<'a> {
    buffer: cobalt_aws::s3::AsyncPutObject<'a>,
}

impl<'a> ManifestFileUpload<'a> {
    pub fn new(client: &'a aws_sdk_s3::Client, dst: &S3Object) -> ManifestFileUpload<'a> {
        let manifest_upload = cobalt_aws::s3::AsyncPutObject::new(
            client,
            &dst.bucket,
            &(dst.key.to_owned() + ".manifest.jsonl"),
        );
        ManifestFileUpload {
            buffer: manifest_upload,
        }
    }

    pub async fn write_manifest_entry(&mut self, entry: &ManifestEntry) -> Result<()> {
        let manifest_entry = serde_json::to_string(&entry)? + "\n";
        self.buffer
            .write_all(manifest_entry.as_bytes())
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn upload_object(&mut self) -> Result<()> {
        self.buffer.close().await.map_err(anyhow::Error::from)
    }
}

const CRC32: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

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

    let mut byte_limit =
        ByteLimit::new_from_inner(upload, MAX_ZIP_FILE_SIZE_BYTES.into()).compat_write();
    let zip = async_zip::write::ZipFileWriter::new(&mut byte_limit);
    let zip = Arc::new(Mutex::new(zip));

    let zip_stream = stream::iter(srcs.into_iter().zip(0_u32..))
        .map(|(src, i)| {
            ensure!(
                i <= MAX_FILES_IN_ZIP.into(),
                "ZIP64 is not supported: Too many zip entries."
            );
            let src = src?;
            // Entry_path is
            let entry_path = src
                .key
                .trim_start_matches(prefix_strip.unwrap_or_default())
                .to_owned();
            ensure!(
                !entry_path.is_empty(),
                "{} with out prefix {prefix_strip:?} is an invalid entry ",
                src.key
            );
            Ok((src, entry_path))
        })
        .map_ok(move |(src, entry_path)| {
            client
                .get_object()
                .bucket(&src.bucket)
                .key(&src.key)
                .send()
                .map_ok(|r| (r, src, entry_path))
                .map_err(anyhow::Error::from)
        })
        .try_buffered(src_fetch_buffer)
        .and_then(|(response, src, entry_path)| {
            let zip = zip.clone();
            async move {
                let crc32 = process_entry(
                    &mut *zip.lock().await,
                    response,
                    &src,
                    &entry_path,
                    compression,
                )
                .await?;
                Ok::<_, anyhow::Error>((crc32, src, entry_path))
            }
        });

    //If manifests are needed fold a upload over the stream
    //Add a Option<S3Object> as manifest dest to args
    if false {
        let mut manifest_upload = ManifestFileUpload::new(client, dst);
        zip_stream
            .try_fold(
                &mut manifest_upload,
                |manifest_upload, (crc32, src, entry_path)| async move {
                    manifest_upload
                        .write_manifest_entry(&ManifestEntry::new(&src, crc32, &entry_path))
                        .await?;
                    Ok(manifest_upload)
                },
            )
            .await?;
        manifest_upload.upload_object().await?;
    } else {
        zip_stream.map_ok(|_| ()).try_collect().await?;
    }

    let zip =
        Arc::try_unwrap(zip).map_err(|_| anyhow::Error::msg("Failed to unwrap ZipFileWriter"))?;
    let zip = zip.into_inner();
    zip.close().await?;
    ////The zip writer does not close the multipart upload
    byte_limit.shutdown().await?;
    Ok(())
}

async fn process_entry<T: tokio::io::AsyncWrite + Unpin>(
    zip: &mut async_zip::write::ZipFileWriter<T>,
    mut response: GetObjectOutput,
    src: &S3Object,
    entry_path: &str,
    compression: Compression,
) -> Result<u32> {
    ensure!(response.content_length() <= MAX_FILE_IN_ZIP_SIZE_BYTES.into(),
                "ZIP64 is not supported: Max file size is {MAX_FILE_IN_ZIP_SIZE_BYTES}, {src:?} is {} bytes", response.content_length);
    let opts = ZipEntryBuilder::new(entry_path.to_owned(), compression.into());
    let mut entry_writer = zip.write_entry_stream(opts).await?;

    //crc is needed for validation
    let mut crc_sink = crate::counter::CRC32Sink::new(&CRC32);

    while let Some(bytes) = response.body.next().await {
        let bytes = bytes?;
        entry_writer.write_all(&bytes).await?;
        crc_sink.send(bytes).await?;
    }
    // If this is not done the Zip file produced silently corrupts
    entry_writer.close().await?;
    crc_sink.close().await?;
    let crc = crc_sink
        .value()
        .context("Expected CRC Sink to have value")?;
    Ok(crc)
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
