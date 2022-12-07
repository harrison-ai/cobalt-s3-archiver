//! Allows ZIP archives to be created in S3 from files stored in S3.
pub mod aws;

use std::sync::Arc;

use anyhow::Error;
use anyhow::{ensure, Context, Result};
use async_zip::{Compression as AsyncCompression, ZipEntryBuilder};
use aws_sdk_s3::output::GetObjectOutput;
use bytesize::MIB;
use clap::ValueEnum;
use cobalt_async::checksum::CRC32Sink;
use cobalt_async::counter::ByteLimit;
use cobalt_aws::s3::S3Object;
use cobalt_aws::s3::{AsyncMultipartUpload, AsyncPutObject};
use futures::future;
use futures::lock::Mutex;
use futures::prelude::*;
use futures::stream;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_stream::wrappers::LinesStream;
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use typed_builder::TypedBuilder;

/// The compression algorithms that are available to be used
/// to compress entries in the ZIP archive.
#[derive(Debug, Clone, ValueEnum, Copy, PartialEq, Eq)]
pub enum Compression {
    /// No compression.
    Stored,
    /// Use Deflate compression.
    Deflate,
    /// Use Bzip2 compression.
    Bzip,
    /// Use LZMA compression.
    Lzma,
    /// Use ZSTD compression.
    Zstd,
    /// Use XZ compression.
    Xz,
}

/// Conversion from [Compression] into [async_zip::Compression]
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

/// Maximum files allowed in a non ZIP64 ZIP file.
const MAX_FILES_IN_ZIP: u16 = u16::MAX;
/// Maximum size of file allowed allowed in a non ZIP64 ZIP file.
const MAX_FILE_IN_ZIP_SIZE_BYTES: u32 = u32::MAX;
/// Maximum total size of ZIP file in non ZIP64 ZIP file.
const MAX_ZIP_FILE_SIZE_BYTES: u32 = u32::MAX;

/// A single entry in the manifest file which describes
/// a single entry in the output ZIP archive.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestEntry {
    /// S3 Object of the source object for this entry.
    pub object: S3Object,
    /// The file name of the entry in the ZIP archive.
    pub filename_in_zip: String,
    /// CRC32 of the source file.
    pub crc32: u32,
}

impl ManifestEntry {
    /// Create a new [ManifestEntry] using the provided values.
    pub fn new(object: &S3Object, crc32: u32, filename_in_zip: &str) -> Self {
        ManifestEntry {
            object: object.clone(),
            crc32,
            filename_in_zip: filename_in_zip.to_owned(),
        }
    }
}

/// Serializes a sequence of [ManifestEntry] instances into S3 as a JSONL file.
///
/// This will buffer all [ManifestEntry] records in memory and write to S3 when
/// [ManifestFileUpload::upload_object] is called.
pub struct ManifestFileUpload<'a> {
    buffer: cobalt_aws::s3::AsyncPutObject<'a>,
}

impl<'a> ManifestFileUpload<'a> {
    /// Create a new [ManifestFileUpload] instance, which will write the data into
    /// the `dst` [S3Object] when [ManifestFileUpload::upload_object] is called.
    pub fn new(client: &'a aws_sdk_s3::Client, dst: &S3Object) -> ManifestFileUpload<'a> {
        let manifest_upload = cobalt_aws::s3::AsyncPutObject::new(client, &dst.bucket, &dst.key);
        ManifestFileUpload {
            buffer: manifest_upload,
        }
    }

    /// Adds the `entry` into the buffer
    pub async fn write_manifest_entry(&mut self, entry: &ManifestEntry) -> Result<()> {
        let manifest_entry = serde_json::to_string(&entry)? + "\n";
        self.buffer
            .write_all(manifest_entry.as_bytes())
            .await
            .map_err(Error::from)
    }

    /// Writes the bytes into S3.  Calling this method twice will cause
    /// an [Err] to be returned.
    pub async fn upload_object(&mut self) -> Result<()> {
        self.buffer.close().await.map_err(Error::from)
    }
}

/// Type of write to do. By reading the [Whole] file
/// into memory a `Data descriptor` record is not needed.
enum ZipWrite {
    /// Stream the file into the Zip
    Stream,
    /// Read the entire file before writing into the Zip
    Whole,
}

/// An [Archiver] allows the creation of a ZIP archive
/// in S3 allowing control of what and how the data
/// is written into S3.
#[derive(Debug, TypedBuilder)]
pub struct Archiver<'a> {
    #[builder(default)]
    prefix_strip: Option<&'a str>,
    compression: Compression,
    #[builder(default=5 * bytesize::MIB as usize)]
    part_size: usize,
    #[builder(default = 2)]
    src_fetch_buffer: usize,
    #[builder(default = false)]
    data_descriptors: bool,
}

impl<'a> Archiver<'a> {
    /// Returns the [ZipWrite] type based on
    /// if the [Archiver] was created with
    /// `data_descriptors` set to true of false.
    /// `data_descriptors` set to true will return
    /// [ZipWrite::Stream] else [ZipWrite::Whole].
    fn entry_write_type(&self) -> ZipWrite {
        if self.data_descriptors {
            ZipWrite::Stream
        } else {
            ZipWrite::Whole
        }
    }

    /// Creates a ZIP archive in S3 at the `output_location`
    /// using the `client` from the `srcs` [S3Object]s.
    /// Optionally a `manifest` object is created in S3 which
    /// contains details of the files which have been added
    /// into the archive.
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

        //Create the upload and the zip writer.
        let upload =
            AsyncMultipartUpload::new(client, output_location, self.part_size, None).await?;

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
                    self.entry_write_type()
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
        write_type: ZipWrite,
    ) -> Result<u32> {
        let opts = ZipEntryBuilder::new(entry_path.to_owned(), self.compression.into());

        //crc is needed for validation
        let mut crc_sink = CRC32Sink::default();

        match write_type {
            ZipWrite::Stream => {
                let mut entry_writer = zip.write_entry_stream(opts).await?;
                while let Some(bytes) = response.body.next().await {
                    let bytes = bytes?;
                    entry_writer.write_all(&bytes).await?;
                    crc_sink.send(bytes).await?;
                }
                // If this is not done the Zip file produced silently corrupts
                entry_writer.close().await?;
            }
            ZipWrite::Whole => {
                let bytes = response.body.collect().await?.into_bytes();
                zip.write_entry_whole(opts, &bytes).await?;
                crc_sink.send(bytes).await?;
            }
        }
        crc_sink.close().await?;
        let crc = crc_sink
            .value()
            .context("Expected CRC Sink to have value")?;
        Ok(crc)
    }
}

/// Validates that all the src files from [ManifestEntry] records in the
/// manifest file exist, and that their CRC32 values match the value in the manifest.
/// The `fetch_concurrency` in the number of source files that will
/// be fetched concurrently from S3.
/// The `validate_concurrency` is the number of source files that will
/// be validated concurrently.
pub async fn validate_manifest_file(
    client: &aws_sdk_s3::Client,
    manifest_file: &S3Object,
    fetch_concurrency: usize,
    validate_concurrency: usize,
) -> Result<()> {
    let manifest_lines = client
        .get_object()
        .bucket(&manifest_file.bucket)
        .key(&manifest_file.key)
        .send()
        .map_ok(|r| r.body.into_async_read())
        .map_ok(|l| BufReader::with_capacity(64 * bytesize::KB as usize, l))
        .map_ok(|b| b.lines())
        .await?;

    LinesStream::new(manifest_lines)
        .map_err(anyhow::Error::from)
        .and_then(|l| {
            future::ready(serde_json::from_str::<ManifestEntry>(&l).map_err(anyhow::Error::from))
        })
        .map_ok(move |entry| {
            client
                .get_object()
                .bucket(&entry.object.bucket)
                .key(&entry.object.key)
                .send()
                .map_err(anyhow::Error::from)
                .map_ok(move |r| (r, entry))
        })
        .try_buffered(fetch_concurrency)
        .map_ok(|(r, entry)| async move {
            let mut tokio_sink = FuturesAsyncWriteCompatExt::compat_write(CRC32Sink::default());
            let mut buf =
                BufReader::with_capacity(64 * bytesize::KB as usize, r.body.into_async_read());
            tokio::io::copy(&mut buf, &mut tokio_sink).await?;
            tokio_sink.shutdown().await?;
            let sink_crc32 = tokio_sink
                .into_inner()
                .value()
                .context("Expected a crc32 to be calculated")?;
            ensure!(
                entry.crc32 == sink_crc32,
                "CRC for Entry {:?} in manifest file {:?} was {:?} does not match {sink_crc32}",
                entry.object,
                manifest_file,
                entry.crc32
            );
            Ok(())
        })
        .try_buffered(validate_concurrency)
        .try_collect()
        .await
}

/// Validate each ZIP entry by reading each [ManifestEntry] from the `manifest` file
/// and ensuring that the CRC32 of bytes in the ZIP archive match those recorded in the
/// manifest file.
pub async fn validate_zip_entry_bytes(
    client: &aws_sdk_s3::Client,
    manifest_file: &S3Object,
    zip_file: &S3Object,
) -> Result<()> {
    let manifest_request = client
        .get_object()
        .bucket(&manifest_file.bucket)
        .key(&manifest_file.key)
        .send()
        .map_ok(|r| r.body.into_async_read())
        .map_ok(|r| BufReader::with_capacity(64 * bytesize::KB as usize, r))
        .map_ok(|b| b.lines());

    let zip_request = client
        .get_object()
        .bucket(&zip_file.bucket)
        .key(&zip_file.key)
        .send()
        .map_ok(|r| r.body.into_async_read())
        .map_ok(|r| BufReader::with_capacity(64 * bytesize::KB as usize, r));

    let (manifest_lines, zip_response) = futures::join!(manifest_request, zip_request);
    let mut manifest_lines = manifest_lines?;

    let mut zip_reader = async_zip::read::stream::ZipFileReader::new(zip_response?);

    while !zip_reader.finished() {
        if let Some(reader) = zip_reader.entry_reader().await? {
            let entry_name = reader.entry().filename().to_owned();
            let mut sink = FuturesAsyncWriteCompatExt::compat_write(CRC32Sink::default());
            //Using the stream reader panics with Stored items
            let crc_copy = std::panic::AssertUnwindSafe(
                reader.copy_to_end_crc(&mut sink, 64 * bytesize::KB as usize),
            )
            .catch_unwind()
            .map_err(|_| anyhow::Error::msg("Failed to "));

            let (crc_copy, manifest_line) = futures::join!(crc_copy, manifest_lines.next_line());
            crc_copy??;
            sink.shutdown().await?;

            let manifest_entry = manifest_line?
                .context("Manifest has too few entries")
                .and_then(|l| {
                    serde_json::from_str::<ManifestEntry>(&l).map_err(anyhow::Error::from)
                })?;
            validate_manifest_entry(
                &manifest_entry,
                &entry_name,
                sink.into_inner()
                    .value()
                    .context("Expected a crc32 value")?,
            )?;
        }
    }
    ensure!(
        manifest_lines.next_line().await?.is_none(),
        "Manifest has more entries that the zip."
    );

    Ok(())
}

pub async fn unarchive_all(
    client: &aws_sdk_s3::Client,
    zip_file: &S3Object,
    dst: &S3Object,
) -> Result<()> {
    ensure!(dst.key.ends_with('/'), "destination key must end with `/`");

    let zip_response = client
        .get_object()
        .bucket(&zip_file.bucket)
        .key(&zip_file.key)
        .send()
        .map_ok(|r| r.body.into_async_read())
        .map_ok(|r| BufReader::with_capacity(64 * bytesize::KB as usize, r))
        .await;

    let mut zip_reader = async_zip::read::stream::ZipFileReader::new(zip_response?);
    while !zip_reader.finished() {
        if let Some(reader) = zip_reader.entry_reader().await? {
            let entry = reader.entry();
            let entry_name = entry.filename().to_owned();
            let entry_size = entry.uncompressed_size();
            let dst_key = dst.key.to_owned() + &entry_name;
            //TODO:: Change this to chunk size.
            if entry_size > 5 * MIB as u32 {
                let dst_file = S3Object::new(&dst.bucket, dst_key);
                //TODO:: Pass chunk size as param
                let writer =
                    AsyncMultipartUpload::new(client, &dst_file, 5 * MIB as usize, None).await?;
                let mut tokio_sink = FuturesAsyncWriteCompatExt::compat_write(writer);
                reader
                    .copy_to_end_crc(&mut tokio_sink, 64 * bytesize::KIB as usize)
                    .await?;
                tokio_sink.shutdown().await?;
            } else {
                let writer = AsyncPutObject::new(client, &dst.bucket, &dst_key);
                let mut tokio_sink = FuturesAsyncWriteCompatExt::compat_write(writer);
                reader
                    .copy_to_end_crc(&mut tokio_sink, 64 * bytesize::KIB as usize)
                    .await?;
                tokio_sink.shutdown().await?;
            }
        }
    }

    Ok(())
}

/// Validate that the `filename` and `crc32`
/// match the `manifest_entry`.
fn validate_manifest_entry(
    manifest_entry: &ManifestEntry,
    filename: &str,
    crc32: u32,
) -> Result<()> {
    ensure!(
        manifest_entry.filename_in_zip == filename,
        format!(
            "Validation manifest entry filename {manifest_entry:?} did not match zip {filename}",
        )
    );
    ensure!(
        manifest_entry.crc32 == crc32,
        format!("Validation error manifest entry {manifest_entry:?} crc32 did not match {crc32}",)
    );
    Ok(())
}

/// Validate that the CRC32 value and filenames in each [ManifestEntry]
/// in the `manifest_file` matches those in the central directory
/// of the `zip_file`.
pub async fn validate_zip_central_dir(
    client: &aws_sdk_s3::Client,
    manifest_file: &S3Object,
    zip_file: &S3Object,
) -> Result<()> {
    let manifest_request = client
        .get_object()
        .bucket(&manifest_file.bucket)
        .key(&manifest_file.key)
        .send()
        .map_ok(|r| r.body.into_async_read())
        .map_ok(|l| BufReader::with_capacity(64 * bytesize::KB as usize, l))
        .map_ok(|b| b.lines());

    let zip_request = aws::S3ObjectSeekableRead::new(client, zip_file, None);

    let (manifest_lines, zip_response) = futures::join!(manifest_request, zip_request);
    let mut manifest_lines = manifest_lines?;
    let zip_response = zip_response?;

    let zip_reader = async_zip::read::seek::ZipFileReader::new(zip_response).await?;

    for entry in zip_reader.entries() {
        let manifest_entry = manifest_lines
            .next_line()
            .await?
            .context("Manifest has too few entries")
            .and_then(|l| serde_json::from_str::<ManifestEntry>(&l).map_err(anyhow::Error::from))?;
        validate_manifest_entry(&manifest_entry, entry.filename(), entry.crc32())?
    }
    ensure!(
        manifest_lines.next_line().await?.is_none(),
        "Manifest has more entries that the zip."
    );
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
