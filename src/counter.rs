use std::task::Poll;

use futures::ready;
use futures::AsyncWrite;
use pin_project_lite::pin_project;
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::Context;

pin_project! {
    ///An AyncWrite which counts bytes written.
    pub struct ByteCounter<T:AsyncWrite> {
        byte_count: u128,
        #[pin]
        inner: T
    }
}

impl<T: AsyncWrite> ByteCounter<T> {
    pub fn new(inner: T) -> Self {
        ByteCounter {
            byte_count: 0,
            inner,
        }
    }

    pub fn byte_count(&self) -> u128 {
        self.byte_count
    }

    pub fn to_inner(self) -> T {
        self.inner
    }
}

impl<T: AsyncWrite> AsyncWrite for ByteCounter<T> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        let this = self.project();
        let written = ready!(this.inner.poll_write(cx, buf))?;
        *this.byte_count += u128::try_from(written).map_err(|e| Error::new(ErrorKind::Other, e))?;
        Poll::Ready(Ok(written))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().inner.poll_close(cx)
    }
}

pin_project! {
    ///An AyncWrite which raises an Error if the number of bytes
    ///written is more that the `byte_limit`
    pub struct ByteLimit<T:AsyncWrite> {
        byte_limit: u128,
        #[pin]
        counter: ByteCounter<T>
    }
}

impl<T: AsyncWrite> ByteLimit<T> {
    pub fn new(counter: ByteCounter<T>, byte_limit: u128) -> Self {
        ByteLimit {
            byte_limit,
            counter,
        }
    }

    /// Convenience method to create a `ByteLimit` and `ByteCounter`
    /// from the wrapped inner `AsyncWrite`.
    pub fn new_from_inner(inner: T, byte_limit: u128) -> Self {
        ByteLimit {
            byte_limit,
            counter: ByteCounter::new(inner),
        }
    }

    pub fn to_innner(self) -> ByteCounter<T> {
        self.counter
    }
}

impl<T: AsyncWrite> AsyncWrite for ByteLimit<T> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        let this = self.project();
        let current_count = this.counter.byte_count();
        match this.counter.poll_write(cx, buf)? {
            Poll::Ready(written) => {
                let written_u128 =
                    u128::try_from(written).map_err(|e| Error::new(ErrorKind::Other, e))?;
                if current_count + written_u128 > *this.byte_limit {
                    Poll::Ready(Err(Error::new(
                        ErrorKind::Other,
                        "Byte Limit Reached: {this.byte_limit} bytes",
                    )))
                } else {
                    Poll::Ready(Ok(written))
                }
            }
            _ => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().counter.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.project().counter.poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::prelude::*;

    #[tokio::test]
    async fn test_byte_count() {
        let buffer = vec![];
        let mut counter = ByteCounter::new(buffer);
        let bytes_to_write = 100_usize;
        assert!(counter.write_all(&vec![0; bytes_to_write]).await.is_ok());
        counter.close().await.unwrap();
        assert_eq!(
            u128::try_from(bytes_to_write).unwrap(),
            counter.byte_count()
        );
    }

    #[tokio::test]
    async fn test_byte_count_limit_over() {
        let buffer = vec![];
        let mut counter = ByteLimit::new_from_inner(buffer, 99);
        let bytes_to_write = 100_usize;
        assert!(counter.write_all(&vec![0; bytes_to_write]).await.is_err());
    }

    #[tokio::test]
    async fn test_byte_count_limit_reached() {
        let buffer = vec![];
        let mut counter = ByteLimit::new_from_inner(buffer, 100);
        let bytes_to_write = 100_usize;
        assert!(counter.write_all(&vec![0; bytes_to_write]).await.is_ok());
        let counter = counter.to_innner();
        assert_eq!(counter.byte_count(), bytes_to_write as u128);
    }
}
