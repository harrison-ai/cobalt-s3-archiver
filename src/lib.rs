pub mod aws;
pub mod checksum;
pub mod counter;

use std::str::FromStr;
use std::sync::Arc;

use crate::counter::ByteLimit;
use anyhow::Error;
use anyhow::{bail, ensure, Context, Result};
use async_zip::{Compression as AsyncCompression, ZipEntryBuilder};
use aws::AsyncMultipartUpload;
use aws_sdk_s3::output::GetObjectOutput;
use checksum::CRC32Sink;
use clap::ValueEnum;
use futures::future;
use futures::lock::Mutex;
use futures::prelude::*;
use futures::stream;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use typed_builder::TypedBuilder;
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
    type Error = Error;

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
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse::<Url>()?.try_into()
    }
}

impl TryFrom<String> for S3Object {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse::<Url>()?.try_into()
    }
}

impl FromStr for S3Object {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse::<Url>()?.try_into()
    }
}

impl TryFrom<S3Object> for Url {
    type Error = url::ParseError;
    fn try_from(obj: S3Object) -> std::result::Result<Self, Self::Error> {
        Url::parse(&format!("s3://{}/{}", obj.bucket, obj.key))
    }
}

impl TryFrom<&S3Object> for Url {
    type Error = url::ParseError;
    fn try_from(obj: &S3Object) -> std::result::Result<Self, Self::Error> {
        Url::parse(&format!("s3://{}/{}", obj.bucket, obj.key))
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
        let manifest_upload = cobalt_aws::s3::AsyncPutObject::new(client, &dst.bucket, &dst.key);
        ManifestFileUpload {
            buffer: manifest_upload,
        }
    }

    pub async fn write_manifest_entry(&mut self, entry: &ManifestEntry) -> Result<()> {
        let manifest_entry = serde_json::to_string(&entry)? + "\n";
        self.buffer
            .write_all(manifest_entry.as_bytes())
            .await
            .map_err(Error::from)
    }

    pub async fn upload_object(&mut self) -> Result<()> {
        self.buffer.close().await.map_err(Error::from)
    }
}

#[derive(Debug, TypedBuilder)]
pub struct Archiver<'a> {
    #[builder(default)]
    prefix_strip: Option<&'a str>,
    compression: Compression,
    #[builder(default=5 * bytesize::MIB as usize)]
    part_size: usize,
    #[builder(default = 2)]
    src_fetch_buffer: usize,
}

impl<'a> Archiver<'a> {
    pub async fn create_zip<I>(
        &self,
        client: &aws_sdk_s3::Client,
        srcs: I,
        output_location: &S3Object,
        manifest_object: Option<&S3Object>,
    ) -> Result<()>
    where
        I: IntoIterator<Item = Result<S3Object>>,
    {
        //S3 keys can start with "/" but this is a little confusing
        ensure!(
            self.prefix_strip.filter(|s| s.starts_with('/')).is_none(),
            "prefix_strip must not start with `/`"
        );
        //Fail early to ensure that the dir structure in the Zip does not have '/' at the root.
        ensure!(
            self.prefix_strip.is_none() || self.prefix_strip.filter(|s| s.ends_with('/')).is_some(),
            "prefix_strip must end with `/`"
        );

        println!("Creating zip file from dst_key {:?}", output_location);
        //Create the upload and the zip writer.
        let upload = AsyncMultipartUpload::new(
            client,
            &output_location.bucket,
            &output_location.key,
            self.part_size,
            None,
        )
        .await?;

        let mut byte_limit =
            ByteLimit::new_from_inner(upload, MAX_ZIP_FILE_SIZE_BYTES.into()).compat_write();
        let zip = async_zip::write::ZipFileWriter::new(&mut byte_limit);
        let zip = Arc::new(Mutex::new(zip));

        let zip_stream = stream::iter(srcs.into_iter().zip(1_u64..))
        .map(|(src, src_index)| src.map(|s| (s, src_index)))
        .and_then(
            |(src, src_index)| match src_index <= MAX_FILES_IN_ZIP.into() {
                false => future::err(Error::msg("ZIP64 is not supported: Too many zip entries.")),
                true => future::ok(src),
            },
        ).and_then(|src|{
         future::ok((src
                .key
                .trim_start_matches(self.prefix_strip.unwrap_or_default())
                .to_owned(), src))
        })
        .and_then(|(entry_path, src)| {
           match entry_path.is_empty() {
                true => future::err(Error::msg(format!(
                    "{} with out prefix {:?} is an invalid entry ",
                    src.key, self.prefix_strip
                ))),
                false => future::ok((src, entry_path)),
            }
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
        .try_buffered(self.src_fetch_buffer) //This prefetches the S3 src objects
        .and_then(|(response, src, entry_path)|{
            match response.content_length() > MAX_FILE_IN_ZIP_SIZE_BYTES.into() {
                true => future::err(Error::msg(format!(
                            "ZIP64 is not supported: Max file size is {MAX_FILE_IN_ZIP_SIZE_BYTES}, {src:?} is {} bytes", response.content_length)
                        )),
                false => future::ok((response, src, entry_path))
            }
        })
        .and_then(|(response, src, entry_path)| {
            let zip = zip.clone();
            async move {
                self.process_entry(
                    &mut *zip.lock().await,
                    response,
                    &entry_path,
                ).map_ok(|crc32| ManifestEntry::new(&src, crc32, &entry_path))
                .await
            }
        });

        // If manifests are needed fold a upload over the stream
        match manifest_object {
            Some(object) => {
                zip_stream
                    .try_fold(
                        ManifestFileUpload::new(client, object),
                        |mut manifest_upload, entry| async move {
                            manifest_upload.write_manifest_entry(&entry).await?;
                            Ok(manifest_upload)
                        },
                    )
                    .await?
                    .upload_object()
                    .await?
            }
            None => zip_stream.map_ok(|_| ()).try_collect().await?,
        };

        let zip = Arc::try_unwrap(zip)
            .map_err(|_| anyhow::Error::msg("Failed to unwrap ZipFileWriter"))?;
        let zip = zip.into_inner();
        zip.close().await?;
        //The zip writer does not close the multipart upload
        byte_limit.shutdown().await?;
        Ok(())
    }

    async fn process_entry<T: tokio::io::AsyncWrite + Unpin>(
        &self,
        zip: &mut async_zip::write::ZipFileWriter<T>,
        mut response: GetObjectOutput,
        entry_path: &str,
    ) -> Result<u32> {
        let opts = ZipEntryBuilder::new(entry_path.to_owned(), self.compression.into());
        let mut entry_writer = zip.write_entry_stream(opts).await?;

        //crc is needed for validation
        let mut crc_sink = CRC32Sink::default();

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
}

pub async fn validate_zip(
    client: &aws_sdk_s3::Client,
    manifest_file: &S3Object,
    zip_file: &S3Object,
) -> Result<()> {
    println!("Start validation");
    let manifest_request = client
        .get_object()
        .bucket(&manifest_file.bucket)
        .key(&manifest_file.key)
        .send()
        .map_ok(|r| r.body.into_async_read())
        .map_ok(BufReader::new)
        .map_ok(|b| b.lines());

    println!("Creating SeekableRead");
    let zip_request = crate::aws::S3ObjectSeekableRead::new(client, zip_file);

    let (manifest_lines, zip_response) = futures::join!(manifest_request, zip_request);
    println!("Futures completed");
    let mut manifest_lines = manifest_lines?;

    println!("Creating ZipFile Reader");
    let mut zip_reader = async_zip::read::seek::ZipFileReader::new(zip_response?).await?;
    
    println!("ZipFile Reader starting iterator");
    for index in 0..zip_reader.entries().len() {
        println!("Validating entry {index}");
        let manifest_entry = manifest_lines
            .next_line()
            .await?
            .context("Manifest has too few entries")
            .and_then(|l| serde_json::from_str::<ManifestEntry>(&l).map_err(anyhow::Error::from))?;
        let entry_reader = zip_reader.entry_reader(index).await?;

        let mut sink = FuturesAsyncWriteCompatExt::compat_write(CRC32Sink::default());
        ensure!(
            manifest_entry.filename_in_zip == entry_reader.entry().filename(),
            format!(
                "Validation manifest entry filename {} did not match zip {}",
                manifest_entry.filename_in_zip,
                entry_reader.entry().filename()
            )
        );
        entry_reader
            .copy_to_end_crc(&mut sink, 64 * bytesize::KB as usize)
            .await?;
        sink.shutdown().await?;
        ensure!(
            sink.get_ref().value() == Some(manifest_entry.crc32),
            format!(
                "Validation error manifest entry {manifest_entry:?} crc32 did not match {:?}",
                sink.get_ref().value()
            )
        );
    }

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
