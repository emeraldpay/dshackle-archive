use std::sync::Mutex;
use apache_avro::Codec;
use lazy_static::lazy_static;
use crate::args::{Args, Compression};

lazy_static! {
    pub static ref SHUTDOWN: shutdown::Shutdown = shutdown::Shutdown::new().expect("Failed to create a shutdown hook");
    static ref COMPRESSION: Mutex<Compression> = Mutex::new(Compression::Zstd);
    static ref DRY_RUN: Mutex<bool> = Mutex::new(false);
}

pub fn get_shutdown() -> shutdown::Shutdown {
    SHUTDOWN.clone()
}

pub fn get_avro_codec() -> Codec {
    let compression = COMPRESSION.lock().unwrap();
    match *compression {
        Compression::Snappy => Codec::Snappy,
        Compression::Zstd => Codec::Zstandard,
    }
}

pub fn set_compression(args: &Args) {
    let compression = args.compression.clone().unwrap_or(Compression::Zstd);
    let mut comp = COMPRESSION.lock().unwrap();
    *comp = compression.clone();
}

pub fn is_dry_run() -> bool {
    let dry_run = DRY_RUN.lock().unwrap();
    *dry_run
}

pub fn set_dry_run(args: &Args) {
    let dry_run = args.dry_run;
    let mut dr = DRY_RUN.lock().unwrap();
    *dr = dry_run;
}
