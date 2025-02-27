use std::io::Read;
use anyhow::anyhow;
use apache_avro::Schema;
use apache_avro::types::Record;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use crate::{avros, global};

pub(super) fn consume_sync<R: Read + Send + 'static>(schema: &'static Schema, reader: R) -> UnboundedReceiver<Record<'static>> {
    let (tx, rx) = unbounded_channel();

    tokio::task::spawn_blocking(move || {
        tracing::trace!("Reading avro file...");
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
            if let Err(e) = tx.send(record) {
                tracing::error!("Error sending record to channel: {:?}", e);
                break
            }
        }
        tracing::trace!("Read avro file...");
    });

    rx
}


#[cfg(test)]
mod tests {
    use crate::avros::{BLOCK_SCHEMA, TX_SCHEMA};
    use crate::testing;

    #[tokio::test]
    async fn test_read_btc_723743_block() {
        testing::start_test();
        let file = std::fs::File::open("testdata/fullAvroFiles/000723743.block.avro").unwrap();
        let schema = &BLOCK_SCHEMA;
        let mut rx = super::consume_sync(schema, file);
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
        let mut rx = super::consume_sync(schema, file);
        let mut count = 0;
        while let Some(_record) = rx.recv().await {
            count += 1;
        }
        assert_eq!(count, 2498);
    }
}
