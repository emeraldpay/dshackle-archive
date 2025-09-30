use anyhow::anyhow;
use apache_avro::Schema;
use apache_avro::types::{Record, Value};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref BLOCK_SCHEMA: Schema = Schema::parse_str(r#"
        {
          "name": "Block",
          "namespace": "io.emeraldpay.dshackle.archive.avro",
          "type": "record",
          "fields": [
            {
              "name": "blockchainType",
              "type": {
                "name": "BlockchainType",
                "type": "enum",
                "symbols": [
                  "ETHEREUM",
                  "BITCOIN"
                ]
              }
            },
            {
              "name": "blockchainId",
              "type": "string"
            },
            {
              "name": "archiveTimestamp",
              "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
              }
            },
            {
              "name": "height",
              "type": "long"
            },
            {
              "name": "blockId",
              "type": "string"
            },
            {
              "name": "parentId",
              "type": "string"
            },
            {
              "name": "timestamp",
              "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
              }
            },
            {
              "name": "json",
              "type": "bytes"
            },
            {
              "name": "unclesCount",
              "type": "int"
            },
            {
              "name": "uncle0Json",
              "type": [
                "null",
                "bytes"
              ]
            },
            {
              "name": "uncle1Json",
              "type": [
                "null",
                "bytes"
              ]
            }
          ]
        }
    "#).unwrap();

    pub static ref TX_SCHEMA: Schema = Schema::parse_str(r#"
        {
          "name": "Transaction",
          "namespace": "io.emeraldpay.dshackle.archive.avro",
          "type": "record",
          "fields": [
            {
              "name": "blockchainType",
              "type": {
                "name": "BlockchainType",
                "type": "enum",
                "symbols": [
                  "ETHEREUM",
                  "BITCOIN"
                ]
              }
            },
            {
              "name": "blockchainId",
              "type": "string"
            },
            {
              "name": "archiveTimestamp",
              "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
              }
            },
            {
              "name": "height",
              "type": "long"
            },
            {
              "name": "blockId",
              "type": "string"
            },
            {
              "name": "timestamp",
              "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
              }
            },
            {
              "name": "index",
              "type": "long"
            },
            {
              "name": "txid",
              "type": "string"
            },
            {
              "name": "json",
              "type": "bytes"
            },
            {
              "name": "raw",
              "type": "bytes"
            },
            {
              "name": "from",
              "type": [
                "null",
                "string"
              ],
              "default": null
            },
            {
              "name": "to",
              "type": [
                "null",
                "string"
              ],
              "default": null
            },
            {
              "name": "receiptJson",
              "type": [
                "null",
                "bytes"
              ],
              "default": null
            },
            {
              "name": "traceJson",
              "type": [
                "null",
                "bytes"
              ],
              "default": null
            },
            {
              "name": "stateDiffJson",
              "type": [
                "null",
                "bytes"
              ],
              "default": null
            }
          ]
        }
    "#).unwrap();

    pub static ref TX_TRACE_SCHEMA: Schema = Schema::parse_str(r#"
        {
          "name": "TransactionTrace",
          "namespace": "io.emeraldpay.dshackle.archive.avro",
          "type": "record",
          "fields": [
            {
              "name": "blockchainType",
              "type": {
                "name": "BlockchainType",
                "type": "enum",
                "symbols": [
                  "ETHEREUM",
                  "BITCOIN"
                ]
              }
            },
            {
              "name": "blockchainId",
              "type": "string"
            },
            {
              "name": "archiveTimestamp",
              "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
              }
            },
            {
              "name": "height",
              "type": "long"
            },
            {
              "name": "blockId",
              "type": "string"
            },
            {
              "name": "timestamp",
              "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
              }
            },
            {
              "name": "index",
              "type": "long"
            },
            {
              "name": "txid",
              "type": "string"
            },
            {
              "name": "traceJson",
              "type": [
                "null",
                "bytes"
              ],
              "default": null
            },
            {
              "name": "stateDiffJson",
              "type": [
                "null",
                "bytes"
              ],
              "default": null
            }
          ]
        }
    "#).unwrap();
}

pub fn to_record<'s>(schema: &'s Schema, value: Value) -> anyhow::Result<Record<'s>> {
    match value {
        Value::Record(r) => {
            let mut result = Record::new(schema).unwrap();
            for (name, field) in r {
                result.put(name.as_str(), field);
            }
            Ok(result)
        },
        _ => Err(anyhow!("Expected a record, got {:?}", value))
    }
}

pub fn get_height(record: &Record) -> anyhow::Result<u64> {
    let height = record.fields.iter()
        .find(|f| f.0 == "height")
        .ok_or(anyhow!("No height in the record"))?;
    match &height.1 {
        Value::Long(h) => Ok(h.clone() as u64),
        _ => Err(anyhow!("Invalid height type: {:?}", height.1))
    }
}
