use std::io::Read;
use std::sync::mpsc::Receiver;
use std::thread;
use anyhow::anyhow;
use apache_avro::Schema;
use apache_avro::types::Record;
use crate::avros;

pub(super) fn consume_sync<R: Read + Send + 'static>(schema: &'static Schema, reader: R) -> Receiver<Record<'static>> {
    let (tx, rx) = std::sync::mpsc::sync_channel(8);

    thread::spawn(move || {
        let avro_reader = apache_avro::Reader::with_schema(&schema, reader);
        if avro_reader.is_err() {
            tracing::error!("Error reading avro file: {:?}", avro_reader.err().unwrap());
            return
        }
        let mut avro_reader = avro_reader.unwrap();
        while let Some(record) = avro_reader.next() {
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
            }
        }
    });

    rx
}
