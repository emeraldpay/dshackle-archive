// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! `object_store`-backed JSON-per-field target. Same layout as
//! [`crate::storage::json_fs`] but on S3 (or any other `ObjectStore`).
//!
//! Unlike the filesystem variant, dropping a [`JsonObjectsWriter`] without
//! calling [`close`](TargetFileWriter::close) does **not** roll back already-written
//! objects: `object_store`'s async API can't be driven from `Drop` reliably, so we
//! leave the files in place and rely on [`JsonObjectsStorage::find_incomplete_tables`]
//! to detect partial heights on the next Fix run.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use crate::archiver::datakind::{DataKind, DataOptions};
use crate::archiver::filenames::Filenames;
use crate::archiver::range::Range;
use crate::formats::json;
use crate::notify::{FileGroup, Location, RowFiles};
use crate::record::ArchiveRow;
use crate::storage::{ScanTarget, TargetFile, TargetFileWriter, WriteTarget};

pub struct JsonObjectsStorage<S: ObjectStore> {
    os: Arc<S>,
    bucket: String,
    filenames: Filenames,
}

impl<S: ObjectStore> JsonObjectsStorage<S> {
    pub fn new(os: Arc<S>, bucket: String, filenames: Filenames) -> Self {
        Self { os, bucket, filenames }
    }

    fn height_prefix(&self, height: u64) -> String {
        // height_dir returns no trailing slash; add one so `list` scopes the
        // prefix to the height's directory rather than matching sibling
        // directories that happen to share a prefix.
        format!("{}/", self.filenames.height_dir(height))
    }

    async fn list_filenames(&self, height: u64) -> Result<Vec<String>> {
        let prefix = Path::from(self.height_prefix(height));
        let mut stream = self.os.list(Some(&prefix));
        let mut out = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|e| anyhow!("list error: {:?}", e))?;
            if let Some(name) = meta.location.filename() {
                out.push(name.to_string());
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl<S: ObjectStore> WriteTarget for JsonObjectsStorage<S> {
    type Writer = JsonObjectsWriter<S>;

    async fn create(
        &self,
        kind: DataKind,
        range: &Range,
        overwrite: bool,
    ) -> Result<Option<Self::Writer>> {
        Ok(Some(JsonObjectsWriter {
            os: self.os.clone(),
            bucket: self.bucket.clone(),
            filenames: self.filenames.clone(),
            kind,
            range: range.clone(),
            overwrite,
            produced: std::sync::Mutex::new(Vec::new()),
        }))
    }
}

#[async_trait]
impl<S: ObjectStore> ScanTarget for JsonObjectsStorage<S> {
    /// Same heuristic as [`crate::storage::json_fs::JsonFsStorage`]; the per-height
    /// completeness rules live in [`json::missing_kinds_at_height`].
    async fn find_incomplete_tables(
        &self,
        blocks: Range,
        tx_options: &DataOptions,
    ) -> Result<Vec<(Range, Vec<DataKind>)>> {
        let mut per_height: HashMap<u64, Vec<DataKind>> = HashMap::new();
        // Collect heights up front — Range::iter() returns a non-Send iterator
        // we can't hold across an .await.
        let heights: Vec<u64> = blocks.iter().collect();
        for height in heights {
            let entries = self.list_filenames(height).await?;
            let missing = json::missing_kinds_at_height(entries, tx_options);
            if !missing.is_empty() {
                per_height.insert(height, missing);
            }
        }
        Ok(json::collapse_missing(per_height))
    }
}

pub struct JsonObjectsWriter<S: ObjectStore> {
    os: Arc<S>,
    bucket: String,
    filenames: Filenames,
    kind: DataKind,
    range: Range,
    overwrite: bool,
    /// Per-row file groups for the notification report. Only objects actually
    /// written by this session — skipped pre-existing objects were announced
    /// when they were originally written.
    produced: std::sync::Mutex<Vec<RowFiles>>,
}

impl<S: ObjectStore> TargetFile for JsonObjectsWriter<S> {
    fn get_url(&self) -> String {
        let path = match &self.range {
            Range::Single(h) => self.filenames.height_dir(h.height),
            Range::Multiple(start, _) => {
                let dir = self.filenames.height_dir(start.height);
                dir.rsplit_once('/')
                    .map(|(p, _)| p.to_string())
                    .unwrap_or(dir)
            }
        };
        format!("s3://{}/{}", self.bucket, path)
    }
}

#[async_trait]
impl<S: ObjectStore> TargetFileWriter for JsonObjectsWriter<S> {
    async fn append(&self, row: ArchiveRow) -> Result<()> {
        let dir = self.filenames.height_dir(row.height);
        let files = json::encode_row(&row);
        let mut group = FileGroup {
            tx_id: row.tx_id.clone(),
            ..Default::default()
        };
        for file in files {
            let key = format!("{}/{}", dir, file.filename);
            let path = Path::from(key);
            if !self.overwrite {
                if self.os.head(&path).await.is_ok() {
                    tracing::debug!("Skipping existing JSON object: {}", path);
                    continue;
                }
            }
            let payload_len = file.payload.len();
            self.os
                .put(&path, PutPayload::from(Bytes::from(file.payload)))
                .await
                .map_err(|e| anyhow!("Failed to put {}: {:?}", path, e))?;
            crate::progress::on_bytes(payload_len);
            crate::metrics::add_bytes(&self.kind, crate::metrics::Direction::Write, payload_len);
            group.set(file.slot, format!("s3://{}/{}", self.bucket, path));
        }
        if !group.is_empty() {
            self.produced.lock().unwrap().push(RowFiles {
                height: row.height,
                tx_index: row.tx_index,
                group,
            });
        }
        crate::progress::on_record();
        crate::metrics::add_items(&self.kind, crate::metrics::Direction::Write, 1);
        Ok(())
    }

    fn locations(&self) -> Vec<(Range, Location)> {
        Location::files_per_height(self.produced.lock().unwrap().clone())
    }

    async fn close(self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use object_store::memory::InMemory;

    use crate::archiver::datakind::DataKind;
    use crate::record::{BlockchainType, Field};

    fn block_row(height: u64) -> ArchiveRow {
        ArchiveRow {
            kind: DataKind::Blocks,
            blockchain_type: BlockchainType::Ethereum,
            blockchain_id: "ETH".to_string(),
            archive_ts: Utc::now(),
            height,
            block_id: format!("0xblock{}", height),
            timestamp: Utc.timestamp_millis_opt(0).unwrap(),
            parent_id: Some(format!("0xparent{}", height.saturating_sub(1))),
            tx_index: None,
            tx_id: None,
            tx_count: None,
            fields: vec![Field::BlockJson(format!("{{\"h\":{}}}", height).into_bytes())],
        }
    }

    fn tx_row(height: u64, tx_id: &str) -> ArchiveRow {
        ArchiveRow {
            kind: DataKind::Transactions,
            blockchain_type: BlockchainType::Ethereum,
            blockchain_id: "ETH".to_string(),
            archive_ts: Utc::now(),
            height,
            block_id: format!("0xblock{}", height),
            timestamp: Utc.timestamp_millis_opt(0).unwrap(),
            parent_id: None,
            tx_index: Some(0),
            tx_id: Some(tx_id.to_string()),
            tx_count: None,
            fields: vec![
                Field::TxJson(b"{}".to_vec()),
                Field::TxRaw(vec![0xde, 0xad, 0xbe, 0xef]),
                Field::Receipt(b"R".to_vec()),
            ],
        }
    }

    async fn list_paths(mem: &InMemory) -> Vec<String> {
        let mut stream = mem.list(None);
        let mut out = Vec::new();
        while let Some(meta) = stream.next().await {
            if let Ok(meta) = meta {
                out.push(meta.location.to_string());
            }
        }
        out.sort();
        out
    }

    #[tokio::test]
    async fn writes_per_field_objects_under_height_prefix() {
        let mem = Arc::new(InMemory::new());
        let storage = JsonObjectsStorage::new(
            mem.clone(),
            "bucket".to_string(),
            Filenames::with_dir("eth".to_string()),
        );

        let writer = storage
            .create(DataKind::Blocks, &Range::Single(21596362.into()), true)
            .await
            .unwrap()
            .unwrap();
        writer.append(block_row(21596362)).await.unwrap();
        writer.close().await.unwrap();

        let paths = list_paths(&mem).await;
        assert_eq!(paths, vec!["eth/021000000/021596000/021596362/block.json"]);
    }

    #[tokio::test]
    async fn tx_files_use_txid_in_filename_and_keep_raw_hex() {
        let mem = Arc::new(InMemory::new());
        let storage = JsonObjectsStorage::new(
            mem.clone(),
            "bucket".to_string(),
            Filenames::with_dir("eth".to_string()),
        );

        let writer = storage
            .create(DataKind::Transactions, &Range::Single(21596362.into()), true)
            .await
            .unwrap()
            .unwrap();
        writer.append(tx_row(21596362, "0xabc")).await.unwrap();
        writer.close().await.unwrap();

        let raw_path = Path::from("eth/021000000/021596000/021596362/raw-0xabc.hex");
        let raw = mem.get(&raw_path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&raw[..], b"0xdeadbeef");
    }

    #[tokio::test]
    async fn overwrite_false_keeps_existing_object() {
        let mem = Arc::new(InMemory::new());
        let storage = JsonObjectsStorage::new(
            mem.clone(),
            "bucket".to_string(),
            Filenames::with_dir("eth".to_string()),
        );

        let w = storage
            .create(DataKind::Blocks, &Range::Single(21596362.into()), true)
            .await
            .unwrap()
            .unwrap();
        w.append(block_row(21596362)).await.unwrap();
        w.close().await.unwrap();

        let mut row = block_row(21596362);
        row.fields = vec![Field::BlockJson(b"REPLACED".to_vec())];
        let w = storage
            .create(DataKind::Blocks, &Range::Single(21596362.into()), false)
            .await
            .unwrap()
            .unwrap();
        w.append(row).await.unwrap();
        w.close().await.unwrap();

        let path = Path::from("eth/021000000/021596000/021596362/block.json");
        let bytes = mem.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&bytes[..], b"{\"h\":21596362}");
    }

    #[tokio::test]
    async fn find_incomplete_tables_detects_missing_heights() {
        let mem = Arc::new(InMemory::new());
        let storage = JsonObjectsStorage::new(
            mem.clone(),
            "bucket".to_string(),
            Filenames::with_dir("eth".to_string()),
        );

        let writer = storage
            .create(DataKind::Blocks, &Range::Single(100.into()), true)
            .await
            .unwrap()
            .unwrap();
        writer.append(block_row(100)).await.unwrap();
        writer.close().await.unwrap();
        let writer = storage
            .create(DataKind::Transactions, &Range::Single(100.into()), true)
            .await
            .unwrap()
            .unwrap();
        writer.append(tx_row(100, "0xabc")).await.unwrap();
        writer.close().await.unwrap();

        let options = DataOptions::default();
        let incomplete = storage
            .find_incomplete_tables(Range::new(100, 102), &options)
            .await
            .unwrap();
        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].0, Range::new(101, 102));
        let mut kinds = incomplete[0].1.clone();
        kinds.sort();
        assert_eq!(kinds, vec![DataKind::Blocks, DataKind::Transactions]);
    }
}
