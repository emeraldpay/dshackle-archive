// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

use async_trait::async_trait;
use anyhow::Result;

pub mod stream;
pub mod fix;
pub mod verify;
pub mod archive;
pub mod compact;
pub mod report;

///
/// A base trait for Dshackle Archive commands (i.e., for `stream`, `archive`, `compact`, etc.)
#[async_trait]
pub trait CommandExecutor {

    ///
    /// Executes the command
    async fn execute(&self) -> Result<()>;
}
