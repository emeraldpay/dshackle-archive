use std::io::Write;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicUsize;
use anyhow::anyhow;
use apache_avro::types::Record;
use apache_avro::{Codec, Writer};
use async_trait::async_trait;
use futures_util::StreamExt;
use object_store::buffered::BufWriter;
use object_store::ObjectStore;
use object_store::path::Path;
use tokio::{
    sync::{
        mpsc::Receiver,
        oneshot
    },
    io::AsyncWriteExt,
};
use crate::datakind::DataKind;
use crate::filenames::Filenames;
use crate::range::Range;
use crate::storage::{FileReference, TargetFile, TargetStorage};

pub struct ObjectsStorage<S: ObjectStore> {
    os: Arc<S>,
    bucket: String,
    filenames: Filenames,
}

impl<S: ObjectStore>  ObjectsStorage<S>{
    pub fn new(os: S, bucket: String, filenames: Filenames) -> Self {
        Self { os: Arc::new(os), bucket, filenames }
    }
}

#[async_trait]
impl<S: ObjectStore> TargetStorage for ObjectsStorage<S> {
    async fn create(&self, kind: DataKind, range: &Range) -> anyhow::Result<Box<dyn TargetFile + Send + Sync>> {
        let filename = Path::from(self.filenames.path(&kind, range));
        Ok(Box::new(ObjectsFile::new(self.os.clone(), kind, self.bucket.clone(), filename)))
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

struct ObjectsFile<'a> {
    pipe: ObjectWriterPipe,
    writer: Mutex<Writer<'a, ObjectWriterPipe>>,
    closed: oneshot::Receiver<usize>,

    bucket: String,
    path: Path,
}

#[async_trait]
impl TargetFile for ObjectsFile<'_> {

    fn get_url(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.path.to_string())
    }

    async fn append(&self, data: Record<'_>) -> anyhow::Result<()> {
        let mut writer = self.writer.lock().unwrap();
        let _size: usize = writer.append(data).map_err(|e| anyhow!("IO Error: {:?}", e))?;
        Ok(())
    }

    async fn close(self: Box<Self>) -> anyhow::Result<()> {
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

impl ObjectsFile<'_> {
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
    use futures_util::StreamExt;
    use object_store::memory::InMemory;
    use crate::avros::BLOCK_SCHEMA;

    #[tokio::test]
    pub async fn can_write() {
        let mem = Arc::new(InMemory::new());
        let file = Box::new(ObjectsFile::new(mem.clone(), DataKind::Blocks, "test".to_string(), Path::from("test.avro")));

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
}
