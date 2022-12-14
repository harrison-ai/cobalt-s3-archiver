use anyhow::Result;

use async_zip::{read::seek::ZipFileReader, ZipEntry};
use cobalt_aws::s3::S3Object;

use crate::aws::S3ObjectSeekableRead;

/// Provide a method of iterating over the details of archived
/// files without reading the entire archive.
pub struct ZipEntries<'a>(ZipFileReader<S3ObjectSeekableRead<'a>>);

impl<'a> ZipEntries<'a> {
    ///Create a new `ZipEntries` for the `src` object.
    pub async fn new<'b>(
        client: &'b aws_sdk_s3::Client,
        src: &'b S3Object,
        bytes_before_fetch: Option<u64>,
    ) -> Result<ZipEntries<'b>> {
        let s3_seek = S3ObjectSeekableRead::new(client, src, bytes_before_fetch).await?;
        let zip_reader = async_zip::read::seek::ZipFileReader::new(s3_seek).await?;
        Ok(ZipEntries(zip_reader))
    }
}

impl<'a> IntoIterator for &'a ZipEntries<'a> {
    type Item = &'a ZipEntry;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.entries().into_iter()
    }
}
