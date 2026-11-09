// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

use std::fmt::{Display, Formatter};
use std::str::FromStr;

const KB: u64 = 1024;
const MB: u64 = 1024 * KB;
const GB: u64 = 1024 * MB;

/// Amount of memory in bytes.
///
/// Parsed from a number of bytes or a number with a `k`, `m` or `g` suffix (e.g., `512m`, `2g`). The suffixes are
/// binary (`1k` = 1024 bytes) as with Docker and Kubernetes limits, and `kb`/`kib`-style forms are accepted too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MemorySize(u64);

impl From<u64> for MemorySize {
    fn from(value: u64) -> Self {
        MemorySize(value)
    }
}

impl From<MemorySize> for u64 {
    fn from(value: MemorySize) -> Self {
        value.0
    }
}

impl FromStr for MemorySize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_lowercase();
        let digits = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
        let (number, unit) = s.split_at(digits);
        let number: u64 = number.parse()
            .map_err(|_| format!("not a memory size: `{}`, expected a number with an optional k/m/g suffix", s))?;
        let multiplier = match unit.trim() {
            "" | "b" => 1,
            "k" | "kb" | "ki" | "kib" => KB,
            "m" | "mb" | "mi" | "mib" => MB,
            "g" | "gb" | "gi" | "gib" => GB,
            other => return Err(format!("unknown memory unit: `{}`, expected k, m or g", other)),
        };
        let bytes = number.checked_mul(multiplier)
            .ok_or_else(|| format!("memory size is too large: `{}`", s))?;
        if bytes == 0 {
            return Err("memory size must be above zero".to_string());
        }
        Ok(MemorySize(bytes))
    }
}

/// Shows the size in the largest binary unit it reaches, e.g., `1.5GiB` or `512.0MiB`.
impl Display for MemorySize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let bytes = self.0 as f64;
        if self.0 >= GB {
            write!(f, "{:.1}GiB", bytes / GB as f64)
        } else if self.0 >= MB {
            write!(f, "{:.1}MiB", bytes / MB as f64)
        } else if self.0 >= KB {
            write!(f, "{:.1}KiB", bytes / KB as f64)
        } else {
            write!(f, "{}B", self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_bytes() {
        assert_eq!(MemorySize::from_str("1000").unwrap(), MemorySize(1000));
    }

    #[test]
    fn parses_binary_suffixes() {
        assert_eq!(MemorySize::from_str("4k").unwrap(), MemorySize(4 * 1024));
        assert_eq!(MemorySize::from_str("512m").unwrap(), MemorySize(512 * 1024 * 1024));
        assert_eq!(MemorySize::from_str("2G").unwrap(), MemorySize(2 * 1024 * 1024 * 1024));
        assert_eq!(MemorySize::from_str("512MiB").unwrap(), MemorySize(512 * 1024 * 1024));
        assert_eq!(MemorySize::from_str("1 gb").unwrap(), MemorySize(1024 * 1024 * 1024));
    }

    #[test]
    fn rejects_invalid() {
        assert!(MemorySize::from_str("").is_err());
        assert!(MemorySize::from_str("0").is_err());
        assert!(MemorySize::from_str("0g").is_err());
        assert!(MemorySize::from_str("m").is_err());
        assert!(MemorySize::from_str("1.5g").is_err());
        assert!(MemorySize::from_str("10t").is_err());
        assert!(MemorySize::from_str("99999999999999g").is_err());
    }

    #[test]
    fn shows_largest_unit() {
        assert_eq!(MemorySize(100).to_string(), "100B");
        assert_eq!(MemorySize(512 * 1024).to_string(), "512.0KiB");
        assert_eq!(MemorySize(512 * 1024 * 1024).to_string(), "512.0MiB");
        assert_eq!(MemorySize(3 * 512 * 1024 * 1024).to_string(), "1.5GiB");
    }
}
