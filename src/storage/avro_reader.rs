use std::io::Read;
use std::sync::{Arc, Mutex};
use apache_avro::Schema;
use tokio::sync::mpsc;
use crate::{avros, global, metrics};
use crate::archiver::datakind::DataKind;
use crate::storage::{ReadFailure, RecordStream};

/// How many deserialized records the reader may buffer before it blocks waiting
/// for the consumer to catch up. Keeps memory bounded while still allowing the
/// reader thread to stay ahead of the consumer.
const READ_CHANNEL_CAPACITY: usize = 16;

/// Wraps a `Read` and reports bytes read to metrics as they flow through, remembering the
/// failures of the underlying reader. See [`IoFailure`] on why they are kept separately.
struct CountingReader<R> {
    inner: R,
    kind: DataKind,
    io_failure: IoFailure,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.inner.read(buf) {
            Ok(n) => {
                metrics::add_bytes(&self.kind, metrics::Direction::Read, n);
                Ok(n)
            }
            Err(err) => {
                self.io_failure.remember(&err);
                Err(err)
            }
        }
    }
}

///
/// The last failure of the reader under the Avro decoder, shared between the two.
///
/// The decoder reports a broken transfer and a broken file the same way, while for the archive
/// they mean the opposite things: unreadable bytes say nothing about the file, but bytes that
/// were read and don't decode mean the file itself is corrupted. Only the reader knows which
/// of the two happened, so it leaves the answer here.
#[derive(Clone, Default)]
struct IoFailure(Arc<Mutex<Option<String>>>);

impl IoFailure {
    fn remember(&self, err: &std::io::Error) {
        *self.0.lock().unwrap() = Some(err.to_string());
    }

    fn take(&self) -> Option<String> {
        self.0.lock().unwrap().take()
    }
}

///
/// Decode an Avro file into records, reading it on a blocking thread and sending the
/// results through a channel.
///
/// `source` is the URL of the file, used to make the failures traceable back to it and to
/// report them to the consumer.
///
/// Reading stops at the first record that fails, since the Avro decoder keeps no recovery
/// point past a broken block. Whether the consumer learns about it depends on the reason:
/// a file that could not be read sends a [`ReadFailure`] as the last item, while a file that
/// was read but doesn't decode simply ends, i.e. the consumer sees the corrupted file as an
/// incomplete one, which is what it is.
pub(super) fn consume_sync<R: Read + Send + 'static>(kind: DataKind, schema: &'static Schema, source: String, reader: R) -> RecordStream {
    let (tx, rx) = mpsc::channel(READ_CHANNEL_CAPACITY);

    tokio::task::spawn_blocking(move || {
        tracing::trace!("Reading avro file {}...", source);
        let io_failure = IoFailure::default();
        let reader = CountingReader { inner: reader, kind, io_failure: io_failure.clone() };
        let report = |err: String| {
            match io_failure.take() {
                Some(reason) => {
                    tracing::error!("Failed to read {} archive {}: {}", kind, source, reason);
                    let _ = tx.blocking_send(Err(ReadFailure { url: source.clone(), reason }));
                }
                None => tracing::error!("Corrupted {} archive {}: {}", kind, source, err),
            }
        };

        let mut avro_reader = match apache_avro::Reader::with_schema(&schema, reader) {
            Ok(reader) => reader,
            Err(err) => {
                report(format!("Cannot open the file: {}", err));
                return
            }
        };
        let shutdown = global::get_shutdown();
        let mut position = 0usize;
        let mut sent = 0usize;
        while let Some(record) = avro_reader.next() {
            if shutdown.is_signalled() {
                break
            }
            position += 1;
            let record = match record {
                Ok(record) => record,
                Err(err) => {
                    report(format!("Cannot read the record {}: {}", position, err));
                    break
                }
            };
            // A record that doesn't fit the schema is a problem of that record alone, so the rest of the file is still worth reading
            let record = match avros::to_record(&schema, record) {
                Ok(record) => record,
                Err(err) => {
                    tracing::error!("Invalid {} record {} in {}: {}", kind, position, source, err);
                    continue
                }
            };
            // blocking_send applies backpressure — the reader thread pauses when
            // the channel is full, preventing unbounded memory growth
            if tx.blocking_send(Ok(record)).is_err() {
                tracing::trace!("Stopped reading {}, the consumer is gone", source);
                break
            }
            sent += 1;
            metrics::add_items(&kind, metrics::Direction::Read, 1);
        }
        tracing::trace!("Read {} records from {}", sent, source);
    });

    rx
}


#[cfg(test)]
mod tests {
    use std::io::Read;
    use crate::archiver::datakind::DataKind;
    use crate::avros::{BLOCK_SCHEMA, TX_SCHEMA};
    use crate::storage::ReadFailure;
    use crate::testing;

    ///
    /// Gives out a part of the file and then either fails, as a lost connection does, or just
    /// ends, as a file that was stored incomplete.
    struct PartialRead {
        data: Vec<u8>,
        position: usize,
        fails: bool,
    }

    impl Read for PartialRead {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.position >= self.data.len() {
                return if self.fails {
                    Err(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset"))
                } else {
                    Ok(0)
                };
            }
            let len = buf.len().min(self.data.len() - self.position);
            buf[..len].copy_from_slice(&self.data[self.position..self.position + len]);
            self.position += len;
            Ok(len)
        }
    }

    async fn read_half_of_txes(fails: bool) -> Vec<Result<apache_avro::types::Record<'static>, ReadFailure>> {
        let source = "testdata/fullAvroFiles/000723743.txes.avro";
        let content = std::fs::read(source).unwrap();
        let reader = PartialRead {
            data: content[..content.len() / 2].to_vec(),
            position: 0,
            fails,
        };
        let mut rx = super::consume_sync(DataKind::Transactions, &TX_SCHEMA, source.to_string(), reader);
        let mut items = vec![];
        while let Some(item) = rx.recv().await {
            items.push(item);
        }
        items
    }

    #[tokio::test]
    async fn reports_a_file_that_cannot_be_read() {
        testing::start_test();
        let items = read_half_of_txes(true).await;

        let failure = items.last().expect("Nothing read at all");
        assert!(failure.is_err(), "The consumer got no failure, so a partial read looks complete");
        assert!(items[..items.len() - 1].iter().all(|i| i.is_ok()));
        assert!(items.len() > 1, "No records read before the failure");
    }

    ///
    /// A file that was read to the end but doesn't decode is a broken file, and the commands
    /// must be free to repair it, i.e. it must not look like a temporary failure.
    #[tokio::test]
    async fn reports_nothing_for_a_truncated_file() {
        testing::start_test();
        let items = read_half_of_txes(false).await;

        assert!(items.iter().all(|i| i.is_ok()), "A file that was read is reported as unreadable");
        assert!(items.len() > 1, "No records read at all");
        assert!(items.len() < 2498, "The whole file was read");
    }

    #[tokio::test]
    async fn test_read_btc_723743_block() {
        testing::start_test();
        let file = std::fs::File::open("testdata/fullAvroFiles/000723743.block.avro").unwrap();
        let schema = &BLOCK_SCHEMA;
        let mut rx = super::consume_sync(DataKind::Blocks, schema, "testdata/fullAvroFiles/000723743.block.avro".to_string(), file);
        let mut count = 0;
        while let Some(_record) = rx.recv().await {
            count += 1;
        }
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_read_btc_723743_txes() {
        testing::start_test();
        let file = std::fs::File::open("testdata/fullAvroFiles/000723743.txes.avro").unwrap();
        let schema = &TX_SCHEMA;
        let mut rx = super::consume_sync(DataKind::Transactions, schema, "testdata/fullAvroFiles/000723743.txes.avro".to_string(), file);
        let mut count = 0;
        while let Some(_record) = rx.recv().await {
            count += 1;
        }
        assert_eq!(count, 2498);
    }
}
