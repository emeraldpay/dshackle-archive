// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0

//! Summary of a command that goes over a range chunk by chunk, logged once the command is over.
//!
//! A run over a long range logs thousands of lines, and the few chunks that went wrong are easy to miss among them.
//! The report gives totals per outcome, and a sample of the failed ranges with their reasons, which is enough to tell
//! whether the archive needs a follow-up (ex., a Verify or a Fix) without reading the whole log.

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use crate::archiver::range::Range;
use crate::args::Command;

/// How many failed ranges are listed with their reasons. A broken archive can fail on thousands of ranges, usually for
/// the same reason, so keeping all of them would only grow the memory.
const SAMPLE_SIZE: usize = 10;

///
/// Totals of the ranges that ended with the same outcome
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Tally {
    ranges: usize,
    /// The files of the ranges, as the command counts them (ex., the sources of a compaction, or the files written
    /// by a fix). Not every command knows them for every outcome, ex. the Fix doesn't look at the files that are
    /// already archived, so it's not shown instead of being shown as zero.
    files: Option<usize>,
    heights: usize,
}

impl Tally {
    fn add(&mut self, range: &Range, files: Option<usize>) {
        self.ranges += 1;
        if let Some(files) = files {
            self.files = Some(self.files.unwrap_or(0) + files);
        }
        self.heights += range.len();
    }
}

impl Display for Tally {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.files {
            Some(files) => write!(f, "{} ranges, {} files, {} heights", self.ranges, files, self.heights),
            None => write!(f, "{} ranges, {} heights", self.ranges, self.heights),
        }
    }
}

///
/// A range that the command failed to process
#[derive(Debug, Clone)]
struct FailedRange {
    range: Range,
    reason: String,
}

///
/// The report of a single command run.
///
/// `O` is what the command did with a range that didn't fail, ex., compacted it or found it compacted already. Failures
/// are not an outcome of the command but its own part of the report, as each of them comes with a reason.
pub struct RunReport<O> {
    command: Command,
    range: Range,
    /// How many ranges the command was going to process
    planned: usize,
    outcomes: BTreeMap<O, Tally>,
    failed: Tally,
    failures: Vec<FailedRange>,
    interrupted: bool,
}

impl<O: Ord + Copy + Display> RunReport<O> {
    ///
    /// Start the report for a run over the `range`, which the command splits into `planned` ranges (i.e., chunks).
    pub fn new(command: Command, range: Range, planned: usize) -> Self {
        Self {
            command,
            range,
            planned,
            outcomes: BTreeMap::new(),
            failed: Tally::default(),
            failures: Vec::new(),
            interrupted: false,
        }
    }

    ///
    /// Count a range that the command processed, with the number of its files, if the command counts them.
    /// What files are counted depends on the outcome, ex. the sources of a compaction or the files written by a fix.
    pub fn on_range(&mut self, outcome: O, range: &Range, files: Option<usize>) {
        self.outcomes.entry(outcome).or_default().add(range, files);
    }

    ///
    /// Count a range that the command failed to process, with the number of its files, if the command counts them.
    pub fn on_failure(&mut self, range: &Range, files: Option<usize>, reason: impl Display) {
        self.failed.add(range, files);
        if self.failures.len() < SAMPLE_SIZE {
            self.failures.push(FailedRange { range: range.clone(), reason: reason.to_string() });
        }
    }

    ///
    /// Mark the report as covering only the ranges processed before a shutdown
    pub fn on_interrupted(&mut self) {
        self.interrupted = true;
    }

    ///
    /// Log the report, one line per log entry so each of them keeps the log prefix (time, level)
    pub fn log(&self) {
        for line in self.to_string().lines() {
            tracing::info!("{}", line);
        }
    }
}

impl<O: Display> Display for RunReport<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} report for {} ({} ranges)", self.command, self.range, self.planned)?;
        if self.interrupted {
            write!(f, ", interrupted by shutdown")?;
        }
        writeln!(f)?;
        for (outcome, tally) in &self.outcomes {
            writeln!(f, "  {}: {}", outcome, tally)?;
        }
        // shown even when nothing failed, as zero failures is the answer to the most common question about the run
        writeln!(f, "  failed: {}", self.failed)?;
        for failure in &self.failures {
            writeln!(f, "    {}: {}", failure.range, failure.reason)?;
        }
        if self.failed.ranges > self.failures.len() {
            writeln!(f, "    ... and {} more failed ranges", self.failed.ranges - self.failures.len())?;
        }
        // a range can be left without any outcome, ex. when it has no files at all, which would make the totals
        // look like some ranges are lost. On a shutdown it's expected, so it's not shown there.
        let processed = self.outcomes.values().map(|t| t.ranges).sum::<usize>() + self.failed.ranges;
        if !self.interrupted && self.planned > processed {
            writeln!(f, "  not processed: {} ranges", self.planned - processed)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::{Display, Formatter};
    use crate::archiver::range::Range;
    use crate::args::Command;
    use super::RunReport;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum Outcome {
        Done,
        Skipped,
    }

    impl Display for Outcome {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                Outcome::Done => write!(f, "done"),
                Outcome::Skipped => write!(f, "skipped"),
            }
        }
    }

    #[test]
    fn shows_zero_failures() {
        let mut report = RunReport::new(Command::Compact, Range::new(0, 999), 1);
        report.on_range(Outcome::Done, &Range::new(0, 999), Some(10));

        assert_eq!(
            report.to_string(),
            "Compact report for 0..999 (1 ranges)\n\
             \x20 done: 1 ranges, 10 files, 1000 heights\n\
             \x20 failed: 0 ranges, 0 heights\n"
        );
    }

    #[test]
    fn sums_ranges_per_outcome() {
        let mut report = RunReport::new(Command::Compact, Range::new(0, 2999), 3);
        report.on_range(Outcome::Skipped, &Range::new(2000, 2999), Some(1));
        report.on_range(Outcome::Done, &Range::new(0, 999), Some(10));
        report.on_range(Outcome::Done, &Range::new(1000, 1999), Some(12));

        assert_eq!(
            report.to_string(),
            "Compact report for 0..2999 (3 ranges)\n\
             \x20 done: 2 ranges, 22 files, 2000 heights\n\
             \x20 skipped: 1 ranges, 1 files, 1000 heights\n\
             \x20 failed: 0 ranges, 0 heights\n"
        );
    }

    #[test]
    fn lists_failed_ranges_with_reasons() {
        let mut report: RunReport<Outcome> = RunReport::new(Command::Compact, Range::new(0, 1999), 2);
        report.on_failure(&Range::new(0, 999), Some(5), "missing data");
        report.on_failure(&Range::new(1000, 1999), Some(7), "broken file");

        assert_eq!(
            report.to_string(),
            "Compact report for 0..1999 (2 ranges)\n\
             \x20 failed: 2 ranges, 12 files, 2000 heights\n\
             \x20   0..999: missing data\n\
             \x20   1000..1999: broken file\n"
        );
    }

    #[test]
    fn keeps_only_a_sample_of_failures() {
        let mut report: RunReport<Outcome> = RunReport::new(Command::Compact, Range::new(0, 14), 15);
        for height in 0..15 {
            report.on_failure(&Range::single(height), Some(1), "missing data");
        }

        let text = report.to_string();
        assert!(text.contains("  failed: 15 ranges, 15 files, 15 heights\n"));
        assert!(text.contains("    9: missing data\n"));
        assert!(!text.contains("    10: missing data\n"));
        assert!(text.ends_with("    ... and 5 more failed ranges\n"));
    }

    #[test]
    fn marks_interrupted_run() {
        let mut report: RunReport<Outcome> = RunReport::new(Command::Compact, Range::new(0, 999), 1);
        report.on_interrupted();

        assert_eq!(
            report.to_string(),
            "Compact report for 0..999 (1 ranges), interrupted by shutdown\n  failed: 0 ranges, 0 heights\n"
        );
    }

    #[test]
    fn hides_files_when_not_counted() {
        let mut report = RunReport::new(Command::Fix, Range::new(0, 2999), 3);
        report.on_range(Outcome::Done, &Range::new(0, 999), Some(2));
        report.on_range(Outcome::Skipped, &Range::new(1000, 1999), None);
        report.on_failure(&Range::new(2000, 2999), None, "node is down");

        assert_eq!(
            report.to_string(),
            "Fix report for 0..2999 (3 ranges)\n\
             \x20 done: 1 ranges, 2 files, 1000 heights\n\
             \x20 skipped: 1 ranges, 1000 heights\n\
             \x20 failed: 1 ranges, 1000 heights\n\
             \x20   2000..2999: node is down\n"
        );
    }

    #[test]
    fn shows_ranges_without_outcome_after_failures() {
        let mut report: RunReport<Outcome> = RunReport::new(Command::Compact, Range::new(0, 2999), 3);
        report.on_failure(&Range::new(0, 999), Some(5), "missing data");

        assert_eq!(
            report.to_string(),
            "Compact report for 0..2999 (3 ranges)\n\
             \x20 failed: 1 ranges, 5 files, 1000 heights\n\
             \x20   0..999: missing data\n\
             \x20 not processed: 2 ranges\n"
        );
    }

    #[test]
    fn shows_ranges_without_outcome() {
        let mut report: RunReport<Outcome> = RunReport::new(Command::Compact, Range::new(0, 2999), 3);
        report.on_range(Outcome::Done, &Range::new(0, 999), Some(10));

        assert!(report.to_string().ends_with("  not processed: 2 ranges\n"));
    }
}
