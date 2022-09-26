use aws_sdk_s3::error::{CompleteMultipartUploadError, UploadPartError};
use aws_sdk_s3::model::CompletedMultipartUpload;
use aws_sdk_s3::model::CompletedPart;
use aws_sdk_s3::model::ObjectCannedAcl;
use aws_sdk_s3::output::{CompleteMultipartUploadOutput, UploadPartOutput};
use aws_sdk_s3::types::{ByteStream, SdkError};
use futures::future::BoxFuture;
use futures::io::{Error, ErrorKind};
use futures::task::{Context, Poll};
use futures::{ready, AsyncWrite, Future, FutureExt, TryFutureExt};
use std::mem;
use std::pin::Pin;

use aws_sdk_s3::Client;

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

#[derive(Clone)]
struct AsyncMultipartUploadConfig<'a> {
    client: &'a Client,
    bucket: String,
    key: String,
    upload_id: String,
    part_size: usize,
}

pub struct AsyncMultipartUpload<'a> {
    config: AsyncMultipartUploadConfig<'a>,
    state: AsyncMultipartUploadState<'a>,
}

impl<'a> AsyncMultipartUpload<'a> {
    pub async fn new(
        client: &'a Client,
        bucket: &'a str,
        key: &'a str,
    ) -> anyhow::Result<AsyncMultipartUpload<'a>> {
        let result = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .acl(ObjectCannedAcl::BucketOwnerFullControl)
            .send()
            .await?;
        use anyhow::Context;
        let upload_id = result.upload_id().context("Expected Upload Id")?;

        let part_size = 5_usize * 1024_usize.pow(2);
        Ok(AsyncMultipartUpload {
            config: AsyncMultipartUploadConfig {
                client,
                bucket: bucket.into(),
                key: key.into(),
                upload_id: upload_id.into(),
                part_size,
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
        println!("Writing Bytes");
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
                buffer.extend(buf);

                //keep pushing uploads until the buffer is small than the part size
                while buffer.len() >= config.part_size {
                    // |---- buffer ----|--- part ------|
                    //Part is initially bytes more than part size
                    let mut part = buffer.split_off(config.part_size);
                    //We want to upload the first part_size bytes from the buffer
                    //Swap part with buffer so part are now bytes to upload
                    // |--- part ------|--buffer-|
                    std::mem::swap(buffer, &mut part);
                    //Upload a new part
                    let part_upload =
                        AsyncMultipartUpload::upload_part(&config, part, *part_number);
                    uploads.push(part_upload);
                    *part_number += 1;
                }
                //Poll all uploads, remove complete and fetch their results.
                AsyncMultipartUpload::check_uploads(uploads, completed_parts, cx)?;
                Poll::Ready(Ok(buf.len()))
            }
            _ => Poll::Ready(Err(Error::new(
                ErrorKind::Other,
                "Attempted to .write() after .close().",
            ))),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        println!("Flushing bytes");
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
                    cx.waker().wake_by_ref();
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
        println!("Closing");
        let config = self.config.clone();
        match &mut self.state {
            AsyncMultipartUploadState::Writing {
                buffer,
                uploads,
                completed_parts,
                part_number,
            } => {
                println!("Closing: Writing last bytes");
                if !buffer.is_empty() {
                    let buff = mem::take(buffer);
                    let part = AsyncMultipartUpload::upload_part(&config, buff, *part_number);
                    uploads.push(part);
                }
                //Poll all uploads, remove complete and fetch their results.
                AsyncMultipartUpload::check_uploads(uploads, completed_parts, cx)?;
                // Change state to Completeing parts
                self.state = AsyncMultipartUploadState::CompletingParts {
                    uploads: mem::take(uploads),
                    completed_parts: mem::take(completed_parts),
                };
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            AsyncMultipartUploadState::CompletingParts {
                uploads,
                completed_parts,
            } if uploads.is_empty() => {
                //Once uploads are empty change state to Completing
                println!("Closing: All parts uploaded. Completing Multipart");
                let completed_parts = mem::take(completed_parts);
                let completing =
                    AsyncMultipartUpload::complete_multipart_upload(&config, completed_parts);
                self.state = AsyncMultipartUploadState::Completing(completing);
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            AsyncMultipartUploadState::CompletingParts {
                uploads,
                completed_parts,
            } => {
                println!(
                    "Closing: Completing Parts Pending uploads: {:?}",
                    uploads.len()
                );
                //Poll all uploads, remove complete and fetch their results.
                AsyncMultipartUpload::check_uploads(uploads, completed_parts, cx)?;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            AsyncMultipartUploadState::Completing(fut) => {
                //use ready! macro to wait for complete uploaded to be done
                println!("Closing: Completing, waiting for completion of multipart.");
                //ready! is like the ? but for Poll objects returning `Polling` if not Ready
                let result = ready!(Pin::new(fut).poll(cx))
                    .map(|_| ())
                    .map_err(|e| Error::new(ErrorKind::Other, e)); //set state to closed
                println!("Closing: Done");
                self.state = AsyncMultipartUploadState::Closed;
                cx.waker().wake_by_ref();
                Poll::Ready(result)
            }
            AsyncMultipartUploadState::Closed => Poll::Ready(Err(Error::new(
                ErrorKind::Other,
                "Attempted to .flush() writer after .close().",
            ))),
        }
    }
}
