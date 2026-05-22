// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Output-format adapters.
//!
//! Each submodule converts the format-neutral [`crate::record::ArchiveRow`] into a
//! concrete on-the-wire / on-disk representation. Today Avro is the only format;
//! future modules will add JSON-per-field files and Pulsar/Kafka stream encoding.

pub mod avro;
pub mod json;
pub mod stream;
