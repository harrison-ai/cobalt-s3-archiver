use std::task::Poll;

use crc::Crc;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::Context;

pin_project! {
#[must_use = "sinks do nothing unless polled"]
pub struct CRC32Sink<'a, Item> {
    digest: Option<crc::Digest<'a, u32>>,
    value: Option<u32>,
    //Needed to allow StreamExt to determine the type of Item
    marker:  std::marker::PhantomData<Item>
}
}

impl<'a, Item> CRC32Sink<'a, Item> {
    //The generated digest needs to live as long as the crc.
    pub fn new(crc: &'a Crc<u32>) -> CRC32Sink<'a, Item> {
        CRC32Sink {
            digest: Some(crc.digest()),
            value: None,
            marker: std::marker::PhantomData,
        }
    }

    pub fn value(&self) -> Option<u32> {
        self.value
    }
}

impl<'a, Item: AsRef<[u8]>> futures::sink::Sink<Item> for CRC32Sink<'a, Item> {
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
        self: Pin<&mut CRC32Sink<'a, Item>>,
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
        mut self: Pin<&mut CRC32Sink<'a, Item>>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::prelude::*;

    #[tokio::test]
    async fn test_crc32() {
        let crc = Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut sink = CRC32Sink::new(&crc);
        sink.send(&[0, 100]).await.unwrap();
        sink.close().await.unwrap();
        assert_eq!(1463645103, sink.value.unwrap());
    }

    #[tokio::test]
    async fn test_crc32_write_after_close() {
        let crc = Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut sink = CRC32Sink::new(&crc);
        sink.send(&[0, 100]).await.unwrap();
        sink.close().await.unwrap();
        assert!(sink.send(&[0, 100]).await.is_err());
    }

    #[tokio::test]
    async fn test_crc32_flush_after_close() {
        let crc = Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut sink = CRC32Sink::new(&crc);
        sink.send(&[0, 100]).await.unwrap();
        sink.close().await.unwrap();
        assert!(sink.flush().await.is_err());
    }

    #[tokio::test]
    async fn test_crc32_close_after_close() {
        let crc = Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut sink = CRC32Sink::<&[u8]>::new(&crc);
        sink.close().await.unwrap();
        assert!(sink.close().await.is_err());
    }
}
