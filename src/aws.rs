use anyhow::Context as anyContext;
use aws_sdk_s3::error::{CompleteMultipartUploadError, GetObjectError, UploadPartError};
use aws_sdk_s3::model::CompletedMultipartUpload;
use aws_sdk_s3::model::CompletedPart;
use aws_sdk_s3::model::ObjectCannedAcl;
use aws_sdk_s3::output::{CompleteMultipartUploadOutput, GetObjectOutput, UploadPartOutput};
use aws_sdk_s3::types::{ByteStream, SdkError};
use bytesize::{GIB, MIB};
use futures::future::BoxFuture;
use futures::io::{Error, ErrorKind};
use futures::task::{Context, Poll};
use futures::{ready, AsyncWrite, Future, FutureExt, TryFutureExt};
use std::mem;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncSeek};

use aws_sdk_s3::Client;

use crate::S3Object;

type MultipartUploadFuture<'a> =
    BoxFuture<'a, Result<(UploadPartOutput, i32), SdkError<UploadPartError>>>;
type CompleteMultipartUploadFuture<'a> =
    BoxFuture<'a, Result<CompleteMultipartUploadOutput, SdkError<CompleteMultipartUploadError>>>;

enum AsyncMultipartUploadState<'a> {
    ///Bytes are being written
    Writing {
        /// Multipart Uploads that are running
        uploads: Vec<MultipartUploadFuture<'a>>,
        /// Bytes waiting to be written.
        buffer: Vec<u8>,
        /// The next part number to be used.
        part_number: i32,
        /// The completed parts
        completed_parts: Vec<CompletedPart>,
    },
    /// close() has been called and parts are still uploading.
    CompletingParts {
        uploads: Vec<MultipartUploadFuture<'a>>,
        completed_parts: Vec<CompletedPart>,
    },
    /// All parts have been uploaded and the CompleteMultipart is returning.
    Completing(CompleteMultipartUploadFuture<'a>),
    // We have completed writing to S3.
    Closed,
}

#[derive(Clone, Debug)]
struct AsyncMultipartUploadConfig<'a> {
    client: &'a Client,
    bucket: String,
    key: String,
    upload_id: String,
    part_size: usize,
    max_uploading_parts: usize,
}

pub struct AsyncMultipartUpload<'a> {
    config: AsyncMultipartUploadConfig<'a>,
    state: AsyncMultipartUploadState<'a>,
}

const MIN_PART_SIZE: usize = 5_usize * MIB as usize; // 5 Mib
const MAX_PART_SIZE: usize = 5_usize * GIB as usize; // 5 Gib

const DEFAULT_MAX_UPLOADING_PARTS: usize = 100;

impl<'a> AsyncMultipartUpload<'a> {
    pub async fn new(
        client: &'a Client,
        bucket: &'a str,
        key: &'a str,
        part_size: usize,
        max_uploading_parts: Option<usize>,
    ) -> anyhow::Result<AsyncMultipartUpload<'a>> {
        if part_size < MIN_PART_SIZE {
            anyhow::bail!("part_size was {part_size}, can not be less than {MIN_PART_SIZE}")
        }
        if part_size > MAX_PART_SIZE {
            anyhow::bail!("part_size was {part_size}, can not be more than {MAX_PART_SIZE}")
        }

        if max_uploading_parts.unwrap_or(DEFAULT_MAX_UPLOADING_PARTS) == 0 {
            anyhow::bail!("Max uploading parts must not be 0")
        }

        let result = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .acl(ObjectCannedAcl::BucketOwnerFullControl)
            .send()
            .await?;

        use anyhow::Context;
        let upload_id = result.upload_id().context("Expected Upload Id")?;

        Ok(AsyncMultipartUpload {
            config: AsyncMultipartUploadConfig {
                client,
                bucket: bucket.into(),
                key: key.into(),
                upload_id: upload_id.into(),
                part_size,
                max_uploading_parts: max_uploading_parts.unwrap_or(DEFAULT_MAX_UPLOADING_PARTS),
            },
            state: AsyncMultipartUploadState::Writing {
                uploads: vec![],
                buffer: Vec::with_capacity(part_size),
                part_number: 1,
                completed_parts: vec![],
            },
        })
    }

    fn upload_part<'b>(
        config: &AsyncMultipartUploadConfig,
        buffer: Vec<u8>,
        part_number: i32,
    ) -> MultipartUploadFuture<'b> {
        config
            .client
            .upload_part()
            .bucket(&config.bucket)
            .key(&config.key)
            .upload_id(&config.upload_id)
            .part_number(part_number)
            .body(ByteStream::from(buffer))
            .send()
            .map_ok(move |p| (p, part_number))
            .boxed()
    }

    fn poll_all<T>(futures: &mut Vec<BoxFuture<T>>, cx: &mut Context) -> Vec<T> {
        let mut pending = vec![];
        let mut complete = vec![];

        while let Some(mut f) = futures.pop() {
            match Pin::new(&mut f).poll(cx) {
                Poll::Ready(result) => complete.push(result),
                Poll::Pending => pending.push(f),
            }
        }
        futures.extend(pending);
        complete
    }

    fn try_collect_complete_parts(
        complete_results: Vec<Result<(UploadPartOutput, i32), SdkError<UploadPartError>>>,
    ) -> Result<Vec<CompletedPart>, Error> {
        complete_results
            .into_iter()
            .map(|r| r.map_err(|e| Error::new(ErrorKind::Other, e)))
            .map(|r| {
                r.map(|(c, part_number)| {
                    CompletedPart::builder()
                        .set_e_tag(c.e_tag)
                        .part_number(part_number)
                        .build()
                })
            })
            .collect::<Result<Vec<_>, _>>()
    }

    fn complete_multipart_upload<'b>(
        config: &AsyncMultipartUploadConfig,
        completed_parts: Vec<CompletedPart>,
    ) -> CompleteMultipartUploadFuture<'b> {
        config
            .client
            .complete_multipart_upload()
            .key(&config.key)
            .bucket(&config.bucket)
            .upload_id(&config.upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            )
            .send()
            .boxed()
    }

    fn check_uploads(
        uploads: &mut Vec<MultipartUploadFuture<'a>>,
        completed_parts: &mut Vec<CompletedPart>,
        cx: &mut Context,
    ) -> Result<(), Error> {
        let complete_results = AsyncMultipartUpload::poll_all(uploads, cx);
        let new_completed_parts =
            AsyncMultipartUpload::try_collect_complete_parts(complete_results)?;
        completed_parts.extend(new_completed_parts);
        Ok(())
    }
}

impl<'a> AsyncWrite for AsyncMultipartUpload<'a> {
    fn poll_write(
        mut self: Pin<&mut AsyncMultipartUpload<'a>>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        // I'm not sure how to work around borrow of two disjoint fields.
        // I had lifetime issues trying to implement Split Borrows
        let config = self.config.clone();
        match &mut self.state {
            AsyncMultipartUploadState::Writing {
                uploads,
                buffer,
                part_number,
                completed_parts,
            } => {
                //Poll current uploads to make space for in coming data
                AsyncMultipartUpload::check_uploads(uploads, completed_parts, cx)?;
                //only take enough bytes to fill remaining upload capacity
                let upload_capacity = ((config.max_uploading_parts - uploads.len())
                    * config.part_size)
                    - buffer.len();
                let bytes_to_write = std::cmp::min(upload_capacity, buf.len());
                // No capacity to upload
                if bytes_to_write == 0 {
                    uploads.is_empty().then(|| cx.waker().wake_by_ref());
                    return Poll::Pending;
                }
                buffer.extend(&buf[..bytes_to_write]);

                //keep pushing uploads until the buffer is small than the part size
                while buffer.len() >= config.part_size {
                    let mut part = buffer.split_off(config.part_size);
                    // We want to consume the first part of the buffer and upload it to S3.
                    // The split_off call does this but it's the wrong way around.
                    // Use `mem:swap` to reverse the two variables in place.
                    std::mem::swap(buffer, &mut part);
                    //Upload a new part
                    let part_upload =
                        AsyncMultipartUpload::upload_part(&config, part, *part_number);
                    uploads.push(part_upload);
                    *part_number += 1;
                }
                //Poll all uploads, remove complete and fetch their results.
                AsyncMultipartUpload::check_uploads(uploads, completed_parts, cx)?;
                //Return number of bytes written from the input
                Poll::Ready(Ok(bytes_to_write))
            }
            _ => Poll::Ready(Err(Error::new(
                ErrorKind::Other,
                "Attempted to .write() after .close().",
            ))),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        //Ensure all pending uploads are completed.
        match &mut self.state {
            AsyncMultipartUploadState::Writing {
                uploads,
                completed_parts,
                ..
            } => {
                //Poll uploads and mark as completed
                AsyncMultipartUpload::check_uploads(uploads, completed_parts, cx)?;
                if uploads.is_empty() {
                    Poll::Ready(Ok(()))
                } else {
                    //Assume that polled futures will trigger a wake
                    Poll::Pending
                }
            }
            _ => Poll::Ready(Err(Error::new(
                ErrorKind::Other,
                "Attempted to .flush() writer after .close().",
            ))),
        }
    }

    fn poll_close<'b>(
        mut self: Pin<&'b mut AsyncMultipartUpload<'a>>,
        cx: &'b mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        let config = self.config.clone();
        match &mut self.state {
            AsyncMultipartUploadState::Writing {
                buffer,
                uploads,
                completed_parts,
                part_number,
            } => {
                //make space for final upload
                AsyncMultipartUpload::check_uploads(uploads, completed_parts, cx)?;
                if config.max_uploading_parts - uploads.len() == 0 {
                    return Poll::Pending;
                }
                if !buffer.is_empty() {
                    let buff = mem::take(buffer);
                    let part = AsyncMultipartUpload::upload_part(&config, buff, *part_number);
                    uploads.push(part);
                }
                //Poll all uploads, remove complete and fetch their results.
                AsyncMultipartUpload::check_uploads(uploads, completed_parts, cx)?;
                // If no remaining uploads then trigger a wake to move to next state
                uploads.is_empty().then(|| cx.waker().wake_by_ref());
                // Change state to Completing parts
                self.state = AsyncMultipartUploadState::CompletingParts {
                    uploads: mem::take(uploads),
                    completed_parts: mem::take(completed_parts),
                };
                Poll::Pending
            }
            AsyncMultipartUploadState::CompletingParts {
                uploads,
                completed_parts,
            } if uploads.is_empty() => {
                //Once uploads are empty change state to Completing
                let mut completed_parts = mem::take(completed_parts);
                // This was surprising but was needed to complete the upload.
                completed_parts.sort_by_key(|p| p.part_number());
                let completing =
                    AsyncMultipartUpload::complete_multipart_upload(&config, completed_parts);
                self.state = AsyncMultipartUploadState::Completing(completing);
                // Trigger a wake to run with new state and poll the future
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            AsyncMultipartUploadState::CompletingParts {
                uploads,
                completed_parts,
            } => {
                //Poll all uploads, remove complete and fetch their results.
                AsyncMultipartUpload::check_uploads(uploads, completed_parts, cx)?;
                //Trigger a wake if all uploads have completed
                uploads.is_empty().then(|| cx.waker().wake_by_ref());
                Poll::Pending
            }
            AsyncMultipartUploadState::Completing(fut) => {
                //use ready! macro to wait for complete uploaded to be done
                //ready! is like the ? but for Poll objects returning `Polling` if not Ready
                let result = ready!(Pin::new(fut).poll(cx))
                    .map(|_| ())
                    .map_err(|e| Error::new(ErrorKind::Other, e)); //set state to closed
                self.state = AsyncMultipartUploadState::Closed;
                Poll::Ready(result)
            }
            AsyncMultipartUploadState::Closed => Poll::Ready(Err(Error::new(
                ErrorKind::Other,
                "Attempted to .close() writer after .close().",
            ))),
        }
    }
}

type GetObjectFuture<'a> = BoxFuture<'a, Result<GetObjectOutput, SdkError<GetObjectError>>>;

pub struct S3ObjectSeekableRead<'a> {
    client: &'a Client,
    src: &'a S3Object,
    position: u64,
    length: u64,
    state: S3SeekState<'a>,
}

impl<'a> S3ObjectSeekableRead<'a> {
    pub async fn new(
        client: &'a Client,
        src: &'a S3Object,
    ) -> anyhow::Result<S3ObjectSeekableRead<'a>> {
        let length = client
            .head_object()
            .bucket(&src.bucket)
            .key(&src.key)
            .send()
            .map_ok(|i| i.content_length())
            .map_ok(u64::try_from)
            .await??; //Is there a better way to flatten this?
        Ok(Self {
            client,
            src,
            position: 0,
            length,
            state: S3SeekState::default(),
        })
    }
}

enum S3SeekState<'a> {
    Pending,
    Fetching(GetObjectFuture<'a>),
    Reading(Pin<Box<dyn AsyncRead>>),
}

impl<'a> Default for S3SeekState<'a> {
    fn default() -> Self {
        Self::Pending
    }
}

impl<'a> AsyncRead for S3ObjectSeekableRead<'a> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.position >= self.length {
            return Poll::Ready(Ok(()));
        }
        use S3SeekState::*;
        match &mut self.state {
            Pending => {
                println!("New fetch");
                let request = self
                    .client
                    .get_object()
                    .bucket(&self.src.bucket)
                    .key(&self.src.key)
                    .range(format!("bytes={}-", self.position))
                    .send();
                self.state = Fetching(Box::pin(request));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Fetching(request) => match Pin::new(request).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(response)) => {
                    self.state = Reading(Box::pin(response.body.into_async_read()));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(Error::new(ErrorKind::Other, e))),
            },
            Reading(read) => {
                let previous = buf.filled().len();
                match Pin::new(read).poll_read(cx, buf) {
                    Poll::Ready(Ok(())) => {
                        let bytes_read = buf.filled().len() - previous;
                        self.position += u64::try_from(bytes_read)
                            .map_err(|e| Error::new(ErrorKind::Other, e))?;
                        Poll::Ready(Ok(()))
                    }
                    other => other,
                }
            } // Poll Future and move to Pending
        }
    }
}

impl<'a> AsyncSeek for S3ObjectSeekableRead<'a> {
    fn start_seek(mut self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        //Set State to Pending and calculate current possition
        use std::io::SeekFrom::*;

        let new_pos = match position {
            //u64 but S3 API only allow i64
            Start(p) => p,
            End(p) => {
                let new_pos = i128::from(self.length)
                    .checked_add(p.into())
                    .with_context(|| format!("Overflow {} + {p}", self.length))
                    .map_err(|e| Error::new(ErrorKind::Other, e))?;
                if new_pos < 0 {
                    return Err(Error::new(ErrorKind::Other, "Start Seek less than zero "));
                }
                u64::try_from(new_pos).map_err(|e| Error::new(ErrorKind::Other, e))?
            }
            Current(p) => {
                let new_pos = i128::from(self.position)
                    .checked_add(p.into())
                    .with_context(|| format!("Overflow {} + {p}", self.position))
                    .map_err(|e| Error::new(ErrorKind::Other, e))?;
                if new_pos < 0 {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "Seek to position less than zero ",
                    ));
                }
                u64::try_from(new_pos).map_err(|e| Error::new(ErrorKind::Other, e))?
            }
        };

        println!("in {position:?}, old {} new {new_pos}", self.position);

        //Only trigger an S3 request if position has changed
        if new_pos != self.position + 1 {
            self.position = new_pos;
            self.state = S3SeekState::Pending
        };

        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_part_size_too_small() {
        let shared_config = aws_config::load_from_env().await;
        let client = aws_sdk_s3::Client::new(&shared_config);
        assert!(
            AsyncMultipartUpload::new(&client, "bucket", "key", 0_usize, None)
                .await
                .is_err()
        )
    }

    #[tokio::test]
    async fn test_part_size_too_big() {
        let shared_config = aws_config::load_from_env().await;
        let client = aws_sdk_s3::Client::new(&shared_config);
        assert!(
            AsyncMultipartUpload::new(&client, "bucket", "key", 5 * GIB as usize + 1, None)
                .await
                .is_err()
        )
    }

    #[tokio::test]
    async fn test_max_uploading_parts_is_zero() {
        let shared_config = aws_config::load_from_env().await;
        let client = aws_sdk_s3::Client::new(&shared_config);
        assert!(
            AsyncMultipartUpload::new(&client, "bucket", "key", 5 * MIB as usize, Some(0))
                .await
                .is_err()
        )
    }
}
