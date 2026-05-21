// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Backwards-compatible re-exports of the Avro schemas and helpers.
//!
//! The canonical home is [`crate::formats::avro`]; this module is kept as a thin
//! shim so existing call sites compile without churn during the Phase 1 refactor.
//! New code should import from [`crate::formats::avro`] directly.

pub use crate::formats::avro::schema::{
    get_height, to_record, BLOCK_SCHEMA, TX_SCHEMA, TX_TRACE_SCHEMA,
};
