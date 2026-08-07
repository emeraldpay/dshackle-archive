// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Output-format adapters.
//!
//! Each submodule converts the format-neutral [`crate::record::ArchiveRow`] into a
//! concrete on-the-wire / on-disk representation: row-batched Avro files, the
//! per-field JSON layout, and the per-field messages the streaming targets
//! publish.

pub mod avro;
pub mod json;
pub mod stream;
pub mod topics;
