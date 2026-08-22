// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! A blocking [`Read`] over an object in an [`ObjectStore`] that survives a broken body stream.
//!
//! `object_store` recovers from IO on its own, but not from all type of errors (ex., the remote produces an HHTP error)
//! In that case the object is requested anew, from the byte the reading stopped.

use std::io::Read;
use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, GetResult, ObjectStore};
use tokio::runtime::Handle;
use tokio_util::io::{StreamReader, SyncIoBridge};
use crate::global;

/// How many times in a row a broken object body may be re-opened before the read fails.
/// A successful read resets the counter, i.e., it's a limit per failure and not per file.
const MAX_RETRIES: usize = 3;

/// Delay before re-opening the object, multiplied by the number of consecutive failures.
const RETRY_DELAY: Duration = Duration::from_millis(500);

/// The body of a `GET` response as a blocking reader.
type ObjectBody = SyncIoBridge<StreamReader<BoxStream<'static, object_store::Result<Bytes>>, Bytes>>;

///
/// Reads an object from an [`ObjectStore`], re-opening it from the last read position
/// when the response body breaks. See the module docs for the reasoning.
///
/// The reader blocks the current thread, so it must be used from a blocking context
/// (i.e., `tokio::task::spawn_blocking`) and never from an async one.
pub struct ResumableObjectRead {
    os: Arc<dyn ObjectStore>,
    path: Path,
    /// Total size of the object, to recognize a complete read without asking for a range past the end
    size: u64,
    /// Makes a re-opened `GET` fail instead of stitching bytes of two different versions of the object
    etag: Option<String>,
    handle: Handle,
    /// Bytes consumed so far, i.e., where a re-opened `GET` continues from
    position: u64,
    /// `None` after a failure, until the object is re-opened
    body: Option<ObjectBody>,
    consecutive_failures: usize,
}

impl ResumableObjectRead {
    ///
    /// Continue with an already started `GET`, remembering enough of it to re-open the object later.
    ///
    /// Must be called from an async context to capture the current runtime handle.
    pub fn new(os: Arc<dyn ObjectStore>, path: Path, started: GetResult) -> Self {
        let handle = Handle::current();
        let size = started.meta.size;
        let etag = started.meta.e_tag.clone();
        let body = Self::as_reader(started, &handle);
        Self {
            os,
            path,
            size,
            etag,
            handle,
            position: 0,
            body: Some(body),
            consecutive_failures: 0,
        }
    }

    fn as_reader(response: GetResult, handle: &Handle) -> ObjectBody {
        SyncIoBridge::new_with_handle(StreamReader::new(response.into_stream()), handle.clone())
    }

    ///
    /// Request the part of the object that is not read yet.
    fn reopen(&mut self) -> std::io::Result<()> {
        self.wait_before_retry();
        if global::get_shutdown().is_signalled() {
            return Err(std::io::Error::other("Shutting down"));
        }
        let options = GetOptions {
            range: Some(GetRange::Offset(self.position)),
            if_match: self.etag.clone(),
            ..Default::default()
        };
        let response = self.handle
            .block_on(self.os.get_opts(&self.path, options))
            .map_err(std::io::Error::other)?;
        // `if_match` is the actual protection from reading a replaced object, but not every
        // storage reports an ETag and then a changed size is the only difference left to see
        if response.meta.size != self.size {
            return Err(std::io::Error::other(format!(
                "Object changed while reading it: {} bytes instead of {}",
                response.meta.size, self.size
            )));
        }
        self.body = Some(Self::as_reader(response, &self.handle));
        Ok(())
    }

    ///
    /// Wait before the next attempt, proportionally to the number of failures so far.
    /// A shutdown interrupts the waiting, as a retry made while stopping only delays the exit.
    fn wait_before_retry(&self) {
        const CHECK_EVERY: Duration = Duration::from_millis(100);
        let shutdown = global::get_shutdown();
        let mut left = RETRY_DELAY * self.consecutive_failures as u32;
        while !left.is_zero() && !shutdown.is_signalled() {
            let slice = left.min(CHECK_EVERY);
            std::thread::sleep(slice);
            left -= slice;
        }
    }

    ///
    /// Register a failed read or re-open. Returns the error back when there is no retry
    /// left, otherwise logs it and lets the caller continue from the current position.
    fn on_failure(&mut self, err: std::io::Error) -> std::io::Result<()> {
        self.consecutive_failures += 1;
        if self.consecutive_failures > MAX_RETRIES || global::get_shutdown().is_signalled() {
            return Err(err);
        }
        tracing::warn!(
            "Reading {} failed after {} of {} bytes, retrying ({}/{}): {}",
            self.path, self.position, self.size,
            self.consecutive_failures, MAX_RETRIES, err
        );
        Ok(())
    }
}

impl Read for ResumableObjectRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            // A range starting at the end of the object is an error on S3, so the completed
            // read is recognized by the position alone, without asking the storage again
            if self.position >= self.size {
                return Ok(0);
            }
            if self.body.is_none() {
                if let Err(err) = self.reopen() {
                    self.on_failure(err)?;
                    continue;
                }
            }
            match self.body.as_mut().unwrap().read(buf) {
                // A body that ends before the object does loses the same data as a broken one,
                // it just has no error to report; that's how a truncated response looks
                Ok(0) => {
                    self.body = None;
                    let err = std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!("Body ended at {} of {} bytes", self.position, self.size),
                    );
                    self.on_failure(err)?;
                }
                Ok(len) => {
                    self.position += len as u64;
                    self.consecutive_failures = 0;
                    return Ok(len);
                }
                Err(err) => {
                    self.body = None;
                    self.on_failure(err)?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use object_store::memory::InMemory;
    use object_store::{GetResultPayload, ObjectStoreExt, PutPayload};
    use crate::testing;

    const CONTENT_SIZE: usize = 10_000;

    fn content() -> Vec<u8> {
        (0..CONTENT_SIZE as u32).map(|i| (i % 251) as u8).collect()
    }

    ///
    /// Replace the body of a response with one that delivers `ok_bytes` and then fails,
    /// which is how a connection dropped mid-download looks to the reader.
    fn broken_after(response: GetResult, ok_bytes: usize) -> GetResult {
        replace_body(response, ok_bytes, true)
    }

    ///
    /// Replace the body of a response with one that just ends after `ok_bytes`, i.e. a
    /// response truncated on the way without anybody reporting an error.
    fn truncated_after(response: GetResult, ok_bytes: usize) -> GetResult {
        replace_body(response, ok_bytes, false)
    }

    fn replace_body(response: GetResult, ok_bytes: usize, with_error: bool) -> GetResult {
        let meta = response.meta.clone();
        let range = response.range.clone();
        let attributes = response.attributes.clone();
        let extensions = response.extensions.clone();
        let mut remaining = ok_bytes;
        let broken = response.into_stream()
            .flat_map(move |chunk| {
                let head = chunk.map(|data| {
                    let take = data.len().min(remaining);
                    remaining -= take;
                    data.slice(0..take)
                });
                let fail = Err(object_store::Error::Generic {
                    store: "test",
                    source: "connection reset".into(),
                });
                let mut items = match head {
                    Ok(data) if data.is_empty() => vec![],
                    head => vec![head],
                };
                if with_error {
                    items.push(fail);
                }
                futures_util::stream::iter(items)
            })
            .boxed();
        GetResult {
            payload: GetResultPayload::Stream(broken),
            meta,
            range,
            attributes,
            extensions,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resumes_broken_body() {
        testing::start_test();
        let mem = Arc::new(InMemory::new());
        let path = Path::from("test.avro");
        mem.put(&path, PutPayload::from(content())).await.unwrap();

        let started = broken_after(mem.get(&path).await.unwrap(), 8);
        let mut reader = ResumableObjectRead::new(mem, path, started);

        let read = tokio::task::spawn_blocking(move || {
            let mut read = Vec::new();
            reader.read_to_end(&mut read).map(|_| read)
        }).await.unwrap().unwrap();

        assert_eq!(read, content());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resumes_truncated_body() {
        testing::start_test();
        let mem = Arc::new(InMemory::new());
        let path = Path::from("test.avro");
        mem.put(&path, PutPayload::from(content())).await.unwrap();

        let started = truncated_after(mem.get(&path).await.unwrap(), 8);
        let mut reader = ResumableObjectRead::new(mem, path, started);

        let read = tokio::task::spawn_blocking(move || {
            let mut read = Vec::new();
            reader.read_to_end(&mut read).map(|_| read)
        }).await.unwrap().unwrap();

        assert_eq!(read, content());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fails_when_cannot_resume() {
        testing::start_test();
        let mem = Arc::new(InMemory::new());
        let path = Path::from("test.avro");
        mem.put(&path, PutPayload::from(content())).await.unwrap();

        // the object is gone by the time the body breaks, so every attempt to resume fails
        let started = broken_after(mem.get(&path).await.unwrap(), 8);
        let mut reader = ResumableObjectRead::new(mem, Path::from("deleted.avro"), started);

        let read = tokio::task::spawn_blocking(move || {
            let mut read = Vec::new();
            reader.read_to_end(&mut read).map(|_| read)
        }).await.unwrap();

        assert!(read.is_err(), "Expected a failed read, got {:?} bytes", read.map(|d| d.len()));
    }
}
