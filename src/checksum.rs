use std::marker::PhantomData;
use std::task::Poll;

use crc32fast::Hasher;
use futures::AsyncWrite;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::Context;

pin_project! {
#[must_use = "sinks do nothing unless polled"]
pub struct CRC32Sink<Item: AsRef<[u8]>> {
    digest: Option<Hasher>,
    value: Option<u32>,
    //Needed to allow StreamExt to determine the type of Item
    marker:  std::marker::PhantomData<Item>
}
}

impl<Item: AsRef<[u8]>> CRC32Sink<Item> {
    //The generated digest needs to live as long as the crc.
    pub fn new() -> CRC32Sink<Item> {
        CRC32Sink {
            digest: Some(Hasher::new()),
            value: None,
            marker: PhantomData,
        }
    }

    pub fn value(&self) -> Option<u32> {
        self.value
    }
}

impl<Item: AsRef<[u8]>> Default for CRC32Sink<Item> {
    fn default() -> Self {
        Self::new()
    }
}

/// Futures crate provides a into_sink for AsyncWrite but it is
/// not possible to get the value out of it afterwards.
impl<Item: AsRef<[u8]>> futures::sink::Sink<Item> for CRC32Sink<Item> {
    type Error = anyhow::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        match self.digest {
            Some(_) => Poll::Ready(Ok(())),
            None => Poll::Ready(Err(anyhow::Error::msg("Close has been called."))),
        }
    }

    fn start_send(
        self: Pin<&mut CRC32Sink<Item>>,
        item: Item,
    ) -> std::result::Result<(), Self::Error> {
        let mut this = self.project();
        match &mut this.digest {
            Some(digest) => {
                digest.update(item.as_ref());
                Ok(())
            }
            None => Err(anyhow::Error::msg("Close has been called.")),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        match self.digest {
            Some(_) => Poll::Ready(Ok(())),
            None => Poll::Ready(Err(anyhow::Error::msg("Close has been called."))),
        }
    }

    fn poll_close(
        mut self: Pin<&mut CRC32Sink<Item>>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        match std::mem::take(&mut self.digest) {
            Some(digest) => {
                self.value = Some(digest.finalize());
                Poll::Ready(Ok(()))
            }
            None => Poll::Ready(Err(anyhow::Error::msg("Close has been called."))),
        }
    }
}

impl AsyncWrite for CRC32Sink<&[u8]> {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut this = self.project();
        match &mut this.digest {
            Some(digest) => {
                digest.update(buf);
                Poll::Ready(Ok(buf.len()))
            }
            None => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Close has been called",
            ))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let mut this = self.project();
        match &mut this.digest {
            Some(_) => Poll::Ready(Ok(())),
            None => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Close has been called",
            ))),
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match std::mem::take(&mut self.digest) {
            Some(digest) => {
                self.value = Some(digest.finalize());
                Poll::Ready(Ok(()))
            }
            None => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Close has been called",
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::prelude::*;

    #[tokio::test]
    async fn test_crc32() {
        let mut sink = CRC32Sink::default();
        sink.send(&[0, 100]).await.unwrap();
        sink.close().await.unwrap();
        assert_eq!(184989630, sink.value.unwrap());
    }

    #[tokio::test]
    async fn test_crc32_write_after_close() {
        let mut sink = CRC32Sink::default();
        sink.send(&[0, 100]).await.unwrap();
        sink.close().await.unwrap();
        assert!(sink.send(&[0, 100]).await.is_err());
    }

    #[tokio::test]
    async fn test_crc32_flush_after_close() {
        let mut sink = CRC32Sink::default();
        sink.send(&[0, 100]).await.unwrap();
        sink.close().await.unwrap();
        assert!(sink.flush().await.is_err());
    }

    #[tokio::test]
    async fn test_crc32_close_after_close() {
        let mut sink = CRC32Sink::<&[u8]>::default();
        futures::SinkExt::close(&mut sink).await.unwrap();
        assert!(futures::SinkExt::close(&mut sink).await.is_err());
    }
}
