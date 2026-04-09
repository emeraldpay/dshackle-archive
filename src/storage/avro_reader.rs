use std::io::Read;
use anyhow::anyhow;
use apache_avro::Schema;
use apache_avro::types::Record;
use tokio::sync::mpsc;
use crate::{avros, global, metrics};
use crate::archiver::datakind::DataKind;

/// How many deserialized records the reader may buffer before it blocks waiting
/// for the consumer to catch up. Keeps memory bounded while still allowing the
/// reader thread to stay ahead of the consumer.
const READ_CHANNEL_CAPACITY: usize = 16;

/// Wraps a `Read` and reports bytes read to metrics as they flow through.
struct CountingReader<R> {
    inner: R,
    kind: DataKind,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        metrics::add_bytes(&self.kind, metrics::Direction::Read, n);
        Ok(n)
    }
}

pub(super) fn consume_sync<R: Read + Send + 'static>(kind: DataKind, schema: &'static Schema, reader: R) -> mpsc::Receiver<Record<'static>> {
    let (tx, rx) = mpsc::channel(READ_CHANNEL_CAPACITY);

    tokio::task::spawn_blocking(move || {
        tracing::trace!("Reading avro file...");
        let reader = CountingReader { inner: reader, kind };
        let avro_reader = apache_avro::Reader::with_schema(&schema, reader);
        if avro_reader.is_err() {
            tracing::error!("Error reading avro file: {:?}", avro_reader.err().unwrap());
            return
        }
        let shutdown = global::get_shutdown();
        let mut avro_reader = avro_reader.unwrap();
        while let Some(record) = avro_reader.next() {
            if shutdown.is_signalled() {
                break
            }
            let record = record
                .map_err(|e| anyhow!("Error reading record: {:?}", e));
            if record.is_err() {
                tracing::error!("Error reading record: {:?}", record.err().unwrap());
                continue
            }
            let record = record.unwrap();
            let record = avros::to_record(&schema, record);
            if record.is_err() {
                tracing::error!("Error building a record: {:?}", record.err().unwrap());
                continue
            }
            let record = record.unwrap();
            // blocking_send applies backpressure — the reader thread pauses when
            // the channel is full, preventing unbounded memory growth
            if let Err(e) = tx.blocking_send(record) {
                tracing::error!("Error sending record to channel: {:?}", e);
                break
            }
            metrics::add_items(&kind, metrics::Direction::Read, 1);
        }
        tracing::trace!("Read avro file...");
    });

    rx
}


#[cfg(test)]
mod tests {
    use crate::archiver::datakind::DataKind;
    use crate::avros::{BLOCK_SCHEMA, TX_SCHEMA};
    use crate::testing;

    #[tokio::test]
    async fn test_read_btc_723743_block() {
        testing::start_test();
        let file = std::fs::File::open("testdata/fullAvroFiles/000723743.block.avro").unwrap();
        let schema = &BLOCK_SCHEMA;
        let mut rx = super::consume_sync(DataKind::Blocks, schema, file);
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
        let mut rx = super::consume_sync(DataKind::Transactions, schema, file);
        let mut count = 0;
        while let Some(_record) = rx.recv().await {
            count += 1;
        }
        assert_eq!(count, 2498);
    }
}
