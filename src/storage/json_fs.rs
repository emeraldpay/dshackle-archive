// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Filesystem storage that writes one JSON file per [`crate::record::Field`].
//!
//! Layout matches the existing two-level height bucketing
//! (`<parent>/<L1>/<L2>/<HEIGHT>/<file>`). Each archive row produces a set of
//! files inside its height's directory; nothing is grouped into ranges, which is
//! why `compact` and `verify` are not supported for this layout.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use crate::archiver::datakind::{DataKind, DataOptions};
use crate::archiver::filenames::Filenames;
use crate::archiver::range::Range;
use crate::formats::json;
use crate::record::ArchiveRow;
use crate::storage::{ScanTarget, TargetFile, TargetFileWriter, WriteTarget};

/// Filesystem-backed JSON-per-field target.
///
/// Reuses [`Filenames`] only for its level-1/level-2 height bucketing
/// (`height_dir`); the file extension on [`Filenames`] is unused.
pub struct JsonFsStorage {
    parent_dir: PathBuf,
    filenames: Filenames,
}

impl JsonFsStorage {
    pub fn new(dir: PathBuf, filenames: Filenames) -> Self {
        Self { parent_dir: dir, filenames }
    }

    fn height_dir(&self, height: u64) -> PathBuf {
        self.parent_dir.join(self.filenames.height_dir(height))
    }

    fn read_dir_entries(&self, height: u64) -> Result<Vec<String>> {
        let dir = self.height_dir(height);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let entries = fs::read_dir(&dir)
            .map_err(|e| anyhow!("Cannot read dir {:?}: {}", dir, e))?;
        let mut names = Vec::new();
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                names.push(name.to_string());
            }
        }
        Ok(names)
    }
}

#[async_trait]
impl WriteTarget for JsonFsStorage {
    type Writer = JsonFsWriter;

    async fn create(
        &self,
        kind: DataKind,
        range: &Range,
        overwrite: bool,
    ) -> Result<Option<Self::Writer>> {
        // For per-field JSON we don't pre-create a file; the writer fans out at
        // append time and decides per-file whether to overwrite.
        Ok(Some(JsonFsWriter {
            parent_dir: self.parent_dir.clone(),
            filenames: self.filenames.clone(),
            kind,
            range: range.clone(),
            overwrite,
            written_files: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
        }))
    }
}

#[async_trait]
impl ScanTarget for JsonFsStorage {
    /// Per-height completeness check; the shared rules live in
    /// [`json::missing_kinds_at_height`]. The simple heuristic trades exhaustive
    /// per-tx coverage for not having to parse `block.json` on every Fix run;
    /// heights with partial data are re-archived wholesale by Fix, which then
    /// skips files that already exist (see [`JsonFsWriter::append`]).
    async fn find_incomplete_tables(
        &self,
        blocks: Range,
        tx_options: &DataOptions,
    ) -> Result<Vec<(Range, Vec<DataKind>)>> {
        let mut per_height: HashMap<u64, Vec<DataKind>> = HashMap::new();
        for height in blocks.iter() {
            let entries = self.read_dir_entries(height)?;
            let missing = json::missing_kinds_at_height(entries, tx_options);
            if !missing.is_empty() {
                per_height.insert(height, missing);
            }
        }
        Ok(json::collapse_missing(per_height))
    }
}

/// Writer for one (kind, range) session. Each [`Self::append`] call fans the row
/// into the per-field files that live inside the row's height directory.
///
/// Drop semantics: if [`Self::close`] isn't called (e.g., the archive run was
/// aborted), every file written through this session is removed so that
/// partial output doesn't get mistaken for a complete archive by the Fix
/// command's completeness check.
pub struct JsonFsWriter {
    parent_dir: PathBuf,
    filenames: Filenames,
    kind: DataKind,
    range: Range,
    overwrite: bool,
    written_files: Mutex<Vec<PathBuf>>,
    closed: AtomicBool,
}

impl TargetFile for JsonFsWriter {
    /// For a single-height session, the height directory; for a multi-height
    /// session, the level-2 directory that contains all written heights.
    /// Notifications carry this so consumers know where the new data landed.
    fn get_url(&self) -> String {
        let path = match &self.range {
            Range::Single(h) => self.parent_dir.join(self.filenames.height_dir(h.height)),
            Range::Multiple(start, _) => {
                // Drop the trailing height segment to get the level-2 directory.
                let dir = self.filenames.height_dir(start.height);
                let dir = dir.rsplit_once('/').map(|(p, _)| p.to_string()).unwrap_or(dir);
                self.parent_dir.join(dir)
            }
        };
        let canonical = path.canonicalize().unwrap_or(path);
        format!("file://{}", canonical.to_str().unwrap_or("invalid"))
    }
}

#[async_trait]
impl TargetFileWriter for JsonFsWriter {
    async fn append(&self, row: ArchiveRow) -> Result<()> {
        let dir = self.parent_dir.join(self.filenames.height_dir(row.height));
        fs::create_dir_all(&dir)
            .map_err(|e| anyhow!("Failed to create dir {:?}: {}", dir, e))?;

        let files = json::encode_row(&row);
        for file in files {
            let path = dir.join(&file.filename);
            if !self.overwrite && path.exists() {
                tracing::debug!("Skipping existing JSON file: {:?}", path);
                continue;
            }
            fs::write(&path, &file.payload)
                .map_err(|e| anyhow!("Failed to write {:?}: {}", path, e))?;
            crate::progress::on_bytes(file.payload.len());
            crate::metrics::add_bytes(
                &self.kind,
                crate::metrics::Direction::Write,
                file.payload.len(),
            );
            self.written_files.lock().unwrap().push(path);
        }
        crate::progress::on_record();
        crate::metrics::add_items(&self.kind, crate::metrics::Direction::Write, 1);
        Ok(())
    }

    async fn close(self) -> Result<()> {
        self.closed.store(true, Ordering::Relaxed);
        Ok(())
    }
}

impl Drop for JsonFsWriter {
    fn drop(&mut self) {
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        // Session aborted: roll back every file we touched so partial data
        // doesn't get mistaken for a complete archive.
        for path in self.written_files.lock().unwrap().drain(..) {
            if let Err(e) = fs::remove_file(&path) {
                tracing::error!(
                    "Failed to remove uncommitted JSON file {:?}: {:?}",
                    path,
                    e
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use tempfile::tempdir;

    use crate::archiver::datakind::DataKind;
    use crate::record::{ArchiveRow, BlockchainType, Field};

    fn block_row(height: u64) -> ArchiveRow {
        ArchiveRow {
            kind: DataKind::Blocks,
            blockchain_type: BlockchainType::Ethereum,
            blockchain_id: "ETH".to_string(),
            archive_ts: Utc::now(),
            height,
            block_id: format!("0xblock{}", height),
            timestamp: Utc.timestamp_millis_opt(0).unwrap(),
            parent_id: Some(format!("0xparent{}", height - 1)),
            tx_index: None,
            tx_id: None,
            tx_count: None,
            fields: vec![
                Field::BlockJson(format!("{{\"h\":{}}}", height).into_bytes()),
                Field::Uncle { index: 0, json: b"U0".to_vec() },
            ],
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

    #[tokio::test]
    async fn writes_per_field_files_under_height_dir() {
        let tmp = tempdir().unwrap();
        let storage = JsonFsStorage::new(tmp.path().to_path_buf(), Filenames::with_dir("eth".to_string()));

        let writer = storage
            .create(DataKind::Blocks, &Range::Single(21596362.into()), true)
            .await
            .unwrap()
            .unwrap();
        writer.append(block_row(21596362)).await.unwrap();
        writer.close().await.unwrap();

        let dir = tmp.path().join("eth/021000000/021596000/021596362");
        assert!(dir.join("block.json").exists());
        assert!(dir.join("uncle-0.json").exists());
        assert_eq!(fs::read(dir.join("block.json")).unwrap(), b"{\"h\":21596362}");
    }

    #[tokio::test]
    async fn tx_files_use_txid_in_filename_and_keep_raw_hex() {
        let tmp = tempdir().unwrap();
        let storage = JsonFsStorage::new(tmp.path().to_path_buf(), Filenames::with_dir("eth".to_string()));

        let writer = storage
            .create(DataKind::Transactions, &Range::Single(21596362.into()), true)
            .await
            .unwrap()
            .unwrap();
        writer.append(tx_row(21596362, "0xabc")).await.unwrap();
        writer.close().await.unwrap();

        let dir = tmp.path().join("eth/021000000/021596000/021596362");
        assert!(dir.join("tx-0xabc.json").exists());
        assert!(dir.join("receipt-0xabc.json").exists());
        let raw = dir.join("raw-0xabc.hex");
        assert!(raw.exists());
        assert_eq!(fs::read(raw).unwrap(), b"0xdeadbeef");
    }

    #[tokio::test]
    async fn overwrite_false_keeps_existing_file() {
        let tmp = tempdir().unwrap();
        let storage = JsonFsStorage::new(tmp.path().to_path_buf(), Filenames::with_dir("eth".to_string()));

        // First write
        let w = storage
            .create(DataKind::Blocks, &Range::Single(21596362.into()), true)
            .await
            .unwrap()
            .unwrap();
        w.append(block_row(21596362)).await.unwrap();
        w.close().await.unwrap();

        // Second write with overwrite=false and different payload — should not change file
        let mut row = block_row(21596362);
        row.fields = vec![Field::BlockJson(b"REPLACED".to_vec())];
        let w = storage
            .create(DataKind::Blocks, &Range::Single(21596362.into()), false)
            .await
            .unwrap()
            .unwrap();
        w.append(row).await.unwrap();
        w.close().await.unwrap();

        let block = tmp.path().join("eth/021000000/021596000/021596362/block.json");
        assert_eq!(fs::read(block).unwrap(), b"{\"h\":21596362}");
    }

    #[tokio::test]
    async fn drop_without_close_removes_written_files() {
        let tmp = tempdir().unwrap();
        let storage = JsonFsStorage::new(tmp.path().to_path_buf(), Filenames::with_dir("eth".to_string()));
        {
            let w = storage
                .create(DataKind::Blocks, &Range::Single(21596362.into()), true)
                .await
                .unwrap()
                .unwrap();
            w.append(block_row(21596362)).await.unwrap();
            // Drop without closing.
        }
        let block = tmp.path().join("eth/021000000/021596000/021596362/block.json");
        assert!(!block.exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn archive_command_end_to_end_writes_per_field_files() {
        use crate::archiver::Archiver;
        use crate::args::Args;
        use crate::blockchain::mock::{MockBlock, MockData, MockTx, MockType};
        use crate::command::archive::ArchiveCommand;
        use crate::command::CommandExecutor;
        use std::sync::Arc;

        crate::testing::start_test();
        let tmp = tempdir().unwrap();

        // Mock blockchain with three blocks, each with two transactions.
        let data = MockData::new("TEST");
        let data_provider: Arc<MockData> = Arc::new(data);
        for h in 100..103u64 {
            let txs = vec![format!("0xTX{}-A", h), format!("0xTX{}-B", h)];
            data_provider.add_block(MockBlock {
                height: h,
                hash: format!("0xB{}", h),
                parent: format!("0xB{}", h - 1),
                transactions: txs.clone(),
            });
            for tx in &txs {
                data_provider.add_tx(MockTx { hash: tx.clone() });
            }
        }

        let storage = JsonFsStorage::new(
            tmp.path().to_path_buf(),
            Filenames::with_dir("test".to_string()),
        );
        let archiver: Archiver<MockType, JsonFsStorage> =
            Archiver::new_simple(Arc::new(storage), data_provider);

        let args = Args {
            range: Some("100..102".to_string()),
            range_chunk: Some(10),
            ..Default::default()
        };
        let cmd = ArchiveCommand::new(&args, archiver).unwrap();
        cmd.execute().await.unwrap();

        // Every height should have block.json plus per-tx files.
        for h in 100..103u64 {
            let dir = tmp
                .path()
                .join(format!("test/000000000/000000000/{:09}", h));
            assert!(dir.join("block.json").exists(), "missing block.json for {}", h);
            for letter in ["A", "B"] {
                let tx_id = format!("0xTX{}-{}", h, letter);
                assert!(
                    dir.join(format!("tx-{}.json", tx_id)).exists(),
                    "missing tx-{}.json",
                    tx_id
                );
                assert!(
                    dir.join(format!("raw-{}.hex", tx_id)).exists(),
                    "missing raw-{}.hex",
                    tx_id
                );
            }
        }
    }

    #[tokio::test]
    async fn find_incomplete_tables_flags_missing_block_files() {
        let tmp = tempdir().unwrap();
        let storage = JsonFsStorage::new(tmp.path().to_path_buf(), Filenames::with_dir("eth".to_string()));

        // Write height 100 only (block + tx + raw); leave 101 and 102 missing.
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

        let options = DataOptions::default(); // blocks + tx
        let incomplete = storage
            .find_incomplete_tables(Range::new(100, 102), &options)
            .await
            .unwrap();
        // 101 and 102 are both missing both kinds — should merge into a single range.
        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].0, Range::new(101, 102));
        let mut kinds = incomplete[0].1.clone();
        kinds.sort();
        assert_eq!(kinds, vec![DataKind::Blocks, DataKind::Transactions]);
    }
}
