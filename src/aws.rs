//! Provides ways of interacting with objects in S3.

use anyhow::Context as anyContext;
use aws_sdk_s3::error::GetObjectError;
use aws_sdk_s3::output::GetObjectOutput;
use aws_sdk_s3::types::SdkError;
use bytesize::MIB;
use futures::future::BoxFuture;
use futures::io::{Error, ErrorKind};
use futures::task::{Context, Poll};
use futures::{Future, TryFutureExt};
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncSeek};

use aws_sdk_s3::Client;
use tracing::{event, instrument, Level};

use cobalt_aws::s3::S3Object;

type GetObjectFuture<'a> = BoxFuture<'a, Result<GetObjectOutput, SdkError<GetObjectError>>>;

/// A implementation of `AsyncRead` and `AsyncSeek` for an object in S3.
/// Seeking is achieved using S3 byte range requests.  This implementation
/// is best suited for use cases where the seeks are mostly monotonically increasing.
/// If the next seek is less that `bytes_before_fetch` from the current position,
/// bytes will be read (and discarded) from the current request until that position is reached.
/// Otherwise, a new byte range request will be created starting from the new position.
pub struct S3ObjectSeekableRead<'a> {
    client: &'a Client,
    src: &'a S3Object,
    position: u64,
    length: u64,
    bytes_before_fetch: u64,
    state: S3SeekState<'a>,
}

impl<'a> S3ObjectSeekableRead<'a> {
    #[instrument(skip(client))]
    /// Create a new [S3ObjectSeekableRead]
    ///
    /// * `client` - A [Client] to use
    /// * `src` - The S3 Object to read.
    /// * `bytes_before_fetch` - The number of bytes the new seek position must be
    /// infront of the current position before a new S3 `GetObject` request is made.
    pub async fn new(
        client: &'a Client,
        src: &'a S3Object,
        bytes_before_fetch: Option<u64>,
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
            bytes_before_fetch: bytes_before_fetch.unwrap_or(5 * MIB),
            state: S3SeekState::default(),
        })
    }
}

/// State for [S3ObjectSeekableRead]
enum S3SeekState<'a> {
    /// The Request has not yet started.
    Pending,
    /// The `GetObject` request has been made.
    Fetching(GetObjectFuture<'a>),
    /// The `GetObject` request has completed and
    /// the bytes are being read from S3.
    Reading(Pin<Box<dyn AsyncRead>>),
    /// The bytes are being read from S3 and discarded,
    /// to reach the position for a new seek.
    Seeking(Pin<Box<dyn AsyncRead>>, u64),
    None,
}

impl<'a> Default for S3SeekState<'a> {
    fn default() -> Self {
        Self::Pending
    }
}

impl<'a> AsyncRead for S3ObjectSeekableRead<'a> {
    #[instrument(level = "trace", skip(self, cx, buf))]
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
                event!(Level::DEBUG, "Making Get Object Request");
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
            Fetching(request) => {
                event!(Level::DEBUG, "Polling Get Object Request");
                match Pin::new(request).poll(cx) {
                    Poll::Ready(Ok(response)) => {
                        self.state = Reading(Box::pin(response.body.into_async_read()));
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    other => other
                        .map_err(|e| Error::new(ErrorKind::Other, e))
                        .map_ok(|_| ()),
                }
            }
            Reading(read) => {
                event!(Level::DEBUG, "Polling S3 Object ByteStream");
                let previous = buf.filled().len();
                match Pin::new(read).poll_read(cx, buf) {
                    Poll::Ready(Ok(())) => {
                        let bytes_read = buf.filled().len() - previous;
                        event!(Level::DEBUG, "Read {bytes_read} from stream");
                        self.position += u64::try_from(bytes_read)
                            .map_err(|e| Error::new(ErrorKind::Other, e))?;
                        Poll::Ready(Ok(()))
                    }
                    other => other,
                }
            }
            Seeking(_, _) => Poll::Ready(Err(Error::new(
                ErrorKind::Other,
                "Can not read while seeking.",
            ))),
            None => Poll::Ready(Err(Error::new(ErrorKind::Other, "Invalid State: None"))),
        }
    }
}

impl<'a> AsyncSeek for S3ObjectSeekableRead<'a> {
    #[instrument(level = "trace", skip(self))]
    fn start_seek(mut self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        //Set State to Pending and calculate current position
        use std::io::SeekFrom::*;

        let new_pos = match position {
            //u64 but S3 API only allow i64
            Start(p) => p,
            End(p) => {
                //Adding an i64 to a u64 may overflow so use an i128
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
                //Adding an i64 to a u64 may overflow so use an i128
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

        let state = std::mem::replace(&mut self.state, S3SeekState::None);
        // IS the new position within the seekable distance or should a new request be made?
        let is_seekable_range = new_pos <= self.position + self.bytes_before_fetch
            && new_pos > self.position
            && new_pos < self.length;
        event!(Level::DEBUG, position = ?position, new_position = ?new_pos, is_seekable_range = is_seekable_range, bytes_before_fetch = ?self.bytes_before_fetch);
        if new_pos == self.position {
            event!(Level::DEBUG, "Not change in position");
            self.state = state; // Nothing changes
        } else if let (S3SeekState::Reading(f), true) = (state, is_seekable_range) {
            event!(
                Level::DEBUG,
                "New position in range. Reading current bytestream until {new_pos}"
            );
            self.state = S3SeekState::Seeking(f, new_pos); // This will seek
        } else {
            event!(Level::DEBUG, "Moving to {new_pos}, trigger a new S3 fetch");
            self.position = new_pos; //Trigger a new fetch as this location
            self.state = S3SeekState::Pending
        };

        Ok(())
    }

    #[instrument(level = "trace", skip(self, cx))]
    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        let state = std::mem::replace(&mut self.state, S3SeekState::None);
        match state {
            S3SeekState::None => Poll::Ready(Err(Error::new(
                ErrorKind::Other,
                "Invalid State: State should never be None",
            ))),
            S3SeekState::Seeking(mut read, target) => {
                //Read forward to target
                event!(Level::DEBUG, "Reading byte stream to {target}");
                let bytes_to_seek = usize::try_from(target - self.position)
                    .map_err(|e| Error::new(ErrorKind::Other, e))?;
                event!(Level::DEBUG, "Attempting to read {bytes_to_seek} bytes");
                let mut backing = vec![0; bytes_to_seek];
                let mut buf = tokio::io::ReadBuf::new(&mut backing);
                match Pin::new(&mut read).poll_read(cx, &mut buf) {
                    Poll::Ready(Ok(())) => {
                        self.position += u64::try_from(buf.filled().len())
                            .map_err(|e| Error::new(ErrorKind::Other, e))?;
                        event!(Level::DEBUG, "Read {} bytes", buf.filled().len());
                        if self.position == target {
                            event!(
                                Level::DEBUG,
                                "Reached target {target}.  Setting state from Seeking to Reading"
                            );
                            self.state = S3SeekState::Reading(read);
                            Poll::Ready(Ok(self.position))
                        } else {
                            event!(Level::DEBUG, "Did not reach target {target}.");
                            self.state = S3SeekState::Seeking(read, target);
                            // The read had returned data so a wake needs to be triggerd
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                    }
                    other => {
                        self.state = S3SeekState::Seeking(read, target);
                        other.map_ok(|_| self.position)
                    }
                }
            }
            _ => {
                self.state = state;
                Poll::Ready(Ok(self.position))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytesize::GIB;
    use cobalt_aws::s3::AsyncMultipartUpload;

    use super::*;

    #[tokio::test]
    async fn test_part_size_too_small() {
        let shared_config = aws_config::load_from_env().await;
        let client = aws_sdk_s3::Client::new(&shared_config);
        let dst = S3Object::new("bucket", "key");
        assert!(AsyncMultipartUpload::new(&client, &dst, 0_usize, None)
            .await
            .is_err())
    }

    #[tokio::test]
    async fn test_part_size_too_big() {
        let shared_config = aws_config::load_from_env().await;
        let client = aws_sdk_s3::Client::new(&shared_config);
        let dst = S3Object::new("bucket", "key");
        assert!(
            AsyncMultipartUpload::new(&client, &dst, 5 * GIB as usize + 1, None)
                .await
                .is_err()
        )
    }

    #[tokio::test]
    async fn test_max_uploading_parts_is_zero() {
        let shared_config = aws_config::load_from_env().await;
        let client = aws_sdk_s3::Client::new(&shared_config);
        let dst = S3Object::new("bucket", "key");
        assert!(
            AsyncMultipartUpload::new(&client, &dst, 5 * MIB as usize, Some(0))
                .await
                .is_err()
        )
    }
}
