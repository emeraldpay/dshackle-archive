use std::io::{Write};
use std::sync::{Arc, Mutex};
use anyhow::anyhow;
use apache_avro::types::Record;
use apache_avro::{Codec, Writer};
use async_trait::async_trait;
use futures_util::{StreamExt};
use object_store::buffered::BufWriter;
use object_store::{GetResult, ObjectStore};
use object_store::path::Path;
use tokio::{
    sync::{
        mpsc::Receiver,
        oneshot
    },
    io::AsyncWriteExt
};
use tokio_util::io::{StreamReader, SyncIoBridge};
use crate::datakind::DataKind;
use crate::filenames::Filenames;
use crate::range::Range;
use crate::storage::{avro_reader, copy, FileReference, TargetFile, TargetFileReader, TargetFileWriter, TargetStorage};

pub struct ObjectsStorage<S: ObjectStore> {
    os: Arc<S>,
    bucket: String,
    filenames: Filenames,
}

impl<S: ObjectStore>  ObjectsStorage<S>{
    pub fn new(os: Arc<S>, bucket: String, filenames: Filenames) -> Self {
        Self { os, bucket, filenames }
    }
}

#[async_trait]
impl<S: ObjectStore> TargetStorage for ObjectsStorage<S> {

    type Writer = NewObjectsFile<'static>;
    type Reader = ExisingObjectsFile;

    async fn create(&self, kind: DataKind, range: &Range) -> anyhow::Result<NewObjectsFile<'static>> {
        let filename = Path::from(self.filenames.path(&kind, range));
        Ok(NewObjectsFile::new(self.os.clone(), kind, self.bucket.clone(), filename))
    }

    async fn delete(&self, path: &FileReference) -> anyhow::Result<()> {
        let path = Path::from(path.path.clone());
        let removed = self.os.delete(&path).await;
        if let Err(err) = removed {
            return Err(anyhow!("Failed to remove file: {:?}", err));
        }
        Ok(())
    }

    async fn open(&self, path: &FileReference) -> anyhow::Result<ExisingObjectsFile> {
        let object_path = Path::from(path.path.clone());
        let get_result = self.os.get(&object_path).await?;
        let file = ExisingObjectsFile {
            bucket: self.bucket.clone(),
            path: object_path.clone(),
            kind: path.kind.clone(),
            // stream: get_result,
            stream: Mutex::new(get_result),
        };
        Ok(file)
    }

    fn list(&self, range: Range) -> anyhow::Result<Receiver<FileReference>> {
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let filenames = self.filenames.clone();
        let os = self.os.clone();

        tokio::spawn(async move {
            let mut level = filenames.levels(range.start());
            let mut prev: Option<Path> = None;
            while level.height < range.end() && !tx.is_closed() {
                let path = Path::from(level.dir().as_str());
                if prev.is_some() && prev.as_ref().unwrap() == &path {
                    tracing::error!("Checking the same dir twice");
                    return
                }
                prev = Some(path.clone());
                tracing::debug!("List dir: {:?}", path);
                let mut list = os.list(Some(&path));
                while let Some(next) = list.next().await {
                    if next.is_err() {
                        tracing::warn!("Cannot read dir: {:?}", next.err().unwrap());
                        break
                    }
                    let meta = next.unwrap();
                    if tx.is_closed() {
                        return
                    }
                    let filename = meta.location.filename();
                    if filename.is_none() {
                        continue
                    }
                    let filename = filename.unwrap();
                    let is_archive = Filenames::parse(filename.to_string());
                    if is_archive.is_none() {
                        tracing::debug!("Not an archive: {}", filename);
                        continue
                    }
                    let (kind, file_range) = is_archive.unwrap();
                    if file_range.intersect(&range) {
                        let r = FileReference {
                            range: file_range,
                            kind,
                            path: meta.location.to_string(),
                        };
                        if tx.send(r).await.is_err() {
                            return
                        }
                    }
                }
                level = level.next_l2();
            }
        });

        Ok(rx)
    }
}

pub struct NewObjectsFile<'a> {
    pipe: ObjectWriterPipe,
    writer: Mutex<Writer<'a, ObjectWriterPipe>>,
    closed: oneshot::Receiver<usize>,

    bucket: String,
    path: Path,
}

impl TargetFile for NewObjectsFile<'_> {
    fn get_url(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.path.to_string())
    }
}

#[async_trait]
impl TargetFileWriter for NewObjectsFile<'_> {

    async fn append(&self, data: Record<'_>) -> anyhow::Result<()> {
        let mut writer = self.writer.lock().unwrap();
        let _size: usize = writer.append(data).map_err(|e| anyhow!("IO Error: {:?}", e))?;
        Ok(())
    }

    async fn close(self: Self) -> anyhow::Result<()> {
        // Avro doesn't always write the data to the underlying writer immediately, and needs to be
        // flushed independently before closing the file. Otherwise, the file is correct but missing the last appended record(s).
        // Note that the flush should not be called on each append because in that case it misses some of the optimization/compaction/etc
        // that Avro uses where there are multiple records.
        {
            let mut writer = self.writer.lock().unwrap();
            let _ = writer.flush().map_err(|e| anyhow!("IO Error: {:?}", e))?;
            let _ = self.pipe.0.send(WriteOp::Close);
        }

        let url = self.get_url();
        let total_size = self.closed.await?;
        tracing::trace!("Close object: {} ({} bytes written)", url, total_size);
        Ok(())
    }
}

#[derive(Debug)]
pub struct ExisingObjectsFile {
    bucket: String,
    path: Path,
    kind: DataKind,
    // reader: BoxStream<'static, std::io::Result<Bytes>>,
    // stream: Arc<BoxStream<'static, object_store::Result<Bytes>>>,
    stream: Mutex<GetResult>
}

impl TargetFile for ExisingObjectsFile {
    fn get_url(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.path.to_string())
    }
}

impl TargetFileReader for ExisingObjectsFile {
    fn read(self) -> anyhow::Result<Receiver<Record<'static>>> {
        let path = self.path.clone();
        let kind = self.kind.clone();
        tracing::trace!(path = path.to_string(), "Start reading avro file");

        let stream = self.stream.into_inner().unwrap().into_stream();
        let std_reader = SyncIoBridge::new(StreamReader::new(stream));

        let rx_sync = avro_reader::consume_sync(kind.schema(), std_reader);
        let rx = copy::copy_from_sync(rx_sync);

        Ok(rx)
    }
}

impl NewObjectsFile<'_> {
    fn new(storage: Arc<dyn ObjectStore>, kind: DataKind, bucket: String, path: Path) -> Self {
        tracing::debug!("Create object: s3://{}/{}", bucket, path.to_string());
        let buf = BufWriter::new(storage, path.clone());
        let (closed_tx, closed_rx) = oneshot::channel();
        let pipe = Self::pipe_start(buf, closed_tx);
        let writer = Writer::with_codec(kind.schema(), pipe.clone(), Codec::Snappy);
        Self {
            pipe,
            writer: Mutex::new(writer),
            closed: closed_rx,
            bucket,
            path,
        }
    }

    fn pipe_start(mut buf: BufWriter, on_close: oneshot::Sender<usize>) -> ObjectWriterPipe {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut total_size = 0;
            while let Some(op) = rx.recv().await {
                match op {
                    WriteOp::Data(data) => {
                        match buf.write_all(data.as_slice()).await {
                            Err(e) => {
                                tracing::error!("Error writing to object: {:?}", e);
                                let _ = on_close.send(total_size);
                                return;
                            }
                            Ok(_) => {
                                total_size += data.len();
                            }
                        }
                    }
                    WriteOp::Flush => {
                        if let Err(e) = buf.flush().await {
                            tracing::error!("Error flushing object: {:?}", e);
                            return;
                        }
                    }
                    WriteOp::Close => {
                        if let Err(e) = buf.shutdown().await {
                            tracing::error!("Error flushing object: {:?}", e);
                        }
                        let _ = on_close.send(total_size);
                        return;
                    }
                    WriteOp::Abort => {
                        let _ = buf.abort().await;
                        let _ = on_close.send(total_size);
                        return;
                    }
                }
            }
        });
        ObjectWriterPipe(Arc::new(tx))
    }

}

enum WriteOp {
    Data(Vec<u8>),
    Flush,
    Close,
    Abort,
}

#[derive(Clone)]
struct ObjectWriterPipe(Arc<tokio::sync::mpsc::UnboundedSender<WriteOp>>);

impl Write for ObjectWriterPipe {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.send(WriteOp::Data(buf.to_vec()))
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Already closed"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.send(WriteOp::Flush)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Already closed"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use apache_avro::types::Value;
    use chrono::Utc;
    use object_store::memory::InMemory;
    use object_store::PutPayload;
    use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
    use crate::avros::BLOCK_SCHEMA;
    use futures::stream::StreamExt;
    use crate::testing;

    #[tokio::test]
    pub async fn can_write() {
        let mem = Arc::new(InMemory::new());
        let file = Box::new(NewObjectsFile::new(mem.clone(), DataKind::Blocks, "test".to_string(), Path::from("test.avro")));

        let mut record = Record::new(&BLOCK_SCHEMA).unwrap();
        record.put("blockchainType", "ETHEREUM");
        record.put("blockchainId", "ETH");
        record.put("archiveTimestamp", Utc::now().timestamp_millis());
        record.put("height", 100);
        record.put("blockId", "0xdfe2e70d6c116a541101cecbb256d7402d62125f6ddc9b607d49edc989825c64");
        record.put("parentId", "0xdb10afd3efa45327eb284c83cc925bd9bd7966aea53067c1eebe0724d124ec1e");
        record.put("timestamp", 0x55ba43eb_i64 * 1000);
        record.put("json", Value::Bytes(vec![1, 2, 3]));
        record.put("unclesCount", 0);

        let added = file.append(record).await;
        if let Err(e) = added {
            panic!("Error: {:?}", e);
        }
        let closed = file.close().await;
        if let Err(e) = closed {
            panic!("Error: {:?}", e);
        }

        let mut files_stream = mem.list(None);
        let mut files = vec![];
        while let Some(file) = files_stream.next().await.transpose().unwrap() {
            files.push((file.location.to_string(), file.size));
        }
        println!("Files: {:?}", files);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, "test.avro");
        assert!(files[0].1 > 500);
    }

    async fn list(
        range: Range,
        all: Vec<&str>,
    ) -> Vec<FileReference> {
        let mem = Arc::new(InMemory::new());

        for path in all {
            mem.put(
                &Path::from(path),
                PutPayload::from_static(&[1]),
            ).await.unwrap();
        }

        let storage = ObjectsStorage::new(mem, "test".to_string(), Filenames::with_dir("archive/eth".to_string()));

        let files = storage.list(range);
        if let Err(e) = files {
            panic!("Error: {:?}", e);
        }
        let files = ReceiverStream::new(files.unwrap());
        files.collect::<Vec<FileReference>>().await
    }

    #[tokio::test]
    pub async fn test_list_whole() {
        let files = list(
            Range::new(021000000, 022000000),
            vec![
                "archive/eth/021000000/021596000/021596362.block.avro",
                "archive/eth/021000000/021596000/021596362.txes.avro",
                "archive/eth/021000000/021596000/021596363.block.avro",
                "archive/eth/021000000/021596000/021596363.txes.avro",
            ]
        ).await;

        assert_eq!(files.len(), 4);
        assert_eq!(files[0].path, "archive/eth/021000000/021596000/021596362.block.avro");
        assert_eq!(files[1].path, "archive/eth/021000000/021596000/021596362.txes.avro");
        assert_eq!(files[1].kind, DataKind::Transactions);
        assert_eq!(files[1].range, Range::Single(21596362));
        assert_eq!(files[2].path, "archive/eth/021000000/021596000/021596363.block.avro");
        assert_eq!(files[2].kind, DataKind::Blocks);
        assert_eq!(files[3].range, Range::Single(21596363));
    }

    #[tokio::test]
    pub async fn test_list_part() {
        let files = list(
            Range::new(021596363, 021596364),
            vec![
                "archive/eth/021000000/021596000/021596362.block.avro",
                "archive/eth/021000000/021596000/021596362.txes.avro",
                "archive/eth/021000000/021596000/021596363.block.avro",
                "archive/eth/021000000/021596000/021596363.txes.avro",
                "archive/eth/021000000/021596000/021596364.block.avro",
                "archive/eth/021000000/021596000/021596364.txes.avro",
                "archive/eth/021000000/021596000/021596365.block.avro",
                "archive/eth/021000000/021596000/021596365.txes.avro",
            ]
        ).await;

        assert_eq!(files.len(), 4);
        assert_eq!(files[0].path, "archive/eth/021000000/021596000/021596363.block.avro");
        assert_eq!(files[1].path, "archive/eth/021000000/021596000/021596363.txes.avro");
        assert_eq!(files[2].path, "archive/eth/021000000/021596000/021596364.block.avro");
        assert_eq!(files[3].path, "archive/eth/021000000/021596000/021596364.txes.avro");
    }

    #[tokio::test]
    pub async fn test_list_no_files() {
        let files = list(
            Range::new(021596370, 021596375),
            vec![
                "archive/eth/021000000/021596000/021596362.block.avro",
                "archive/eth/021000000/021596000/021596362.txes.avro",
                "archive/eth/021000000/021596000/021596363.block.avro",
                "archive/eth/021000000/021596000/021596363.txes.avro",
            ]
        ).await;

        assert_eq!(files.len(), 0);
    }

    #[tokio::test]
    pub async fn test_list_multi_levels() {
        let files = list(
            Range::new(21_500_000, 21_600_000),
            vec![
                "archive/eth/021000000/021596000/021596362.block.avro",
                "archive/eth/021000000/021596000/021596362.txes.avro",
                "archive/eth/021000000/021596000/021596363.block.avro",
                "archive/eth/021000000/021596000/021596363.txes.avro",
                "archive/eth/021000000/021597000/021597111.block.avro",
                "archive/eth/021000000/021597000/021597111.txes.avro",
                "archive/eth/021000000/021598000/021598444.txes.avro",
            ]
        ).await;

        assert_eq!(files.len(), 7);
        assert_eq!(files[0].path, "archive/eth/021000000/021596000/021596362.block.avro");
        assert_eq!(files[1].path, "archive/eth/021000000/021596000/021596362.txes.avro");
        assert_eq!(files[6].path, "archive/eth/021000000/021598000/021598444.txes.avro");
    }

    #[tokio::test]
    pub async fn write_and_read() {
        testing::start_test();
        let mem = Arc::new(InMemory::new());
        let path = Path::from("test.avro");
        let bucket = "test".to_string();


        let file = NewObjectsFile::new(mem.clone(), DataKind::Blocks, bucket.clone(), path.clone());
        for i in 0..10_000 {
            let mut record = Record::new(&BLOCK_SCHEMA).unwrap();
            record.put("blockchainType", "ETHEREUM");
            record.put("blockchainId", "ETH");
            record.put("archiveTimestamp", Utc::now().timestamp_millis());
            record.put("height", i);
            record.put("blockId", "0xdfe2e70d6c116a541101cecbb256d7402d62125f6ddc9b607d49edc989825c64");
            record.put("parentId", "0xdb10afd3efa45327eb284c83cc925bd9bd7966aea53067c1eebe0724d124ec1e");
            record.put("timestamp", 0x55ba43eb_i64 * 1000 + i * 12);
            record.put("json", Value::Bytes(vec![1, 2, 3]));
            record.put("unclesCount", 0);
            file.append(record).await.unwrap();
        }
        Box::new(file).close().await.unwrap();

        let file = ExisingObjectsFile {
            bucket,
            path: path.clone(),
            kind: DataKind::Blocks,
            stream: Mutex::new(mem.get(&path).await.unwrap()),
        };

        let mut records_stream = file.read().unwrap();
        let mut records = vec![];
        while let Some(record) = records_stream.recv().await {
            records.push(record);
        }

        assert_eq!(records.len(), 10_000);
    }
}
