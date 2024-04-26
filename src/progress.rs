//! Progress reporting infrastructure

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::{
    borrow::Cow,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

/// CLI progress report of ongoing operations
///
/// To avoid corrupted terminal output, you should not write anything to stdout
/// or stderr yourself as long as a report is being displayed. Please use logs
/// for debug messages.
#[derive(Clone, Debug, Default)]
pub struct ProgressReport(MultiProgress);
//
impl ProgressReport {
    /// Prepare to report progress on the cli
    pub fn new() -> Self {
        Self::default()
    }

    /// Prepare to report on a new asynchronous operation
    pub fn add(
        &self,
        what: impl Into<Cow<'static, str>>,
        config: ProgressConfig,
    ) -> ProgressTracker {
        let what = what.into();
        let ProgressConfig {
            initial_work,
            show_rate_eta,
            can_add_work,
        } = config;
        let mut bar = ProgressBar::new(initial_work.into()).with_prefix(what);
        let style_header = "{prefix} {wide_bar} ";
        let style_trailer = match (initial_work, show_rate_eta) {
            (Work::Steps(_), false) => "{pos}/{len}",
            (Work::Steps(_), true) => "{pos}/{len} ({per_sec})",
            (Work::PercentSteps(_), false) => "{percent:>2}%",
            (Work::PercentSteps(_), true) => "{percent:>2}% (~{eta} left)",
            (Work::Bytes(_), false) => "{decimal_bytes}/{decimal_total_bytes}",
            (Work::Bytes(_), true) => {
                "{decimal_bytes}/{decimal_total_bytes} ({decimal_bytes_per_sec})"
            }
        };
        bar = bar.with_style(
            ProgressStyle::with_template(&format!("{style_header}{style_trailer}"))
                .expect("all styles above should be valid indicatif styles"),
        );
        let added = u64::from(initial_work) > 0;
        if added {
            self.0.add(bar.clone());
        }
        ProgressTracker {
            bar,
            report: self.0.clone(),
            added: Arc::new(AtomicBool::new(added)),
            upcoming: Arc::new(AtomicBool::new(can_add_work)),
        }
    }
}

/// Progress bar configuration
///
/// You will normally want to override at least one of `initial_work` and
/// `no_further_work` from the default.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProgressConfig {
    /// Initial length of the progress bar
    initial_work: Work,

    /// Show the completion rate or estimated remaining time, depending on work
    show_rate_eta: bool,

    /// Can add more work after initial configuration
    can_add_work: bool,
}
//
impl ProgressConfig {
    /// Default configuration, with some initial amount of work
    pub fn new(initial_work: Work) -> Self {
        Self {
            initial_work,
            show_rate_eta: true,
            can_add_work: false,
        }
    }

    /// Disable tracking of step completions
    pub fn dont_show_rate_eta(self) -> Self {
        Self {
            show_rate_eta: false,
            ..self
        }
    }

    /// Enable addition of work after initial configuration
    pub fn allow_adding_work(self) -> Self {
        Self {
            can_add_work: true,
            ..self
        }
    }
}

/// Work whose progression that can be tracked
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum Work {
    /// Steps to be taken, with a precise count display
    Steps(usize),

    /// Steps to be taken, with a percentage-based display
    PercentSteps(usize),

    /// Bytes to be processed
    Bytes(usize),
}
//
impl From<Work> for u64 {
    fn from(value: Work) -> Self {
        let inner = match value {
            Work::Steps(s) => s,
            Work::PercentSteps(p) => p,
            Work::Bytes(b) => b,
        };
        inner as u64
    }
}

/// Mechanism to track progress
#[derive(Clone, Debug)]
pub struct ProgressTracker {
    /// Progress bar for this specific process
    bar: ProgressBar,

    /// Underlying process report
    report: MultiProgress,

    /// Truth that the progress bar has already been added to the report
    added: Arc<AtomicBool>,

    /// Truth that more work can still be added to this progress bar
    upcoming: Arc<AtomicBool>,
}
//
impl ProgressTracker {
    /// Show that a certain amount of progress has been made
    ///
    /// Returns truth that the progress bar has reached its maximum value
    pub fn make_progress(&self, progress: u64) -> bool {
        // Track progress
        self.bar.inc(progress);
        let current = self.bar.position();
        let max = self.bar.length().unwrap_or(0);
        assert!(current <= max, "recorded more progress than expected");

        // Hide progress bar once done
        let finished = current == max && !self.upcoming.load(Ordering::Acquire);
        if finished {
            self.bar.finish_and_clear();
            self.report.remove(&self.bar);
        }
        finished
    }

    /// Reset remaining time estimage
    pub fn reset_eta(&self) {
        self.bar.reset_eta();
    }

    /// Increment the amount of progress that remains to be done
    ///
    /// Note that this operation is disabled by default, and you must enable it
    /// in [`ProgressConfig`]. If you use it, call `no_further_work()` once you
    /// know no further work will be coming.
    pub fn add_work(&self, remaining: u64) {
        assert!(
            self.upcoming.load(Ordering::Acquire),
            "should not increment remaining progress after freeze_remaining"
        );
        if !self.added.swap(true, Ordering::AcqRel) && remaining > 0 {
            self.report.add(self.bar.clone());
        }
        self.bar.inc_length(remaining);
    }

    /// Promise that inc_remaining will not be called anymore
    ///
    /// This allows for the progress bar to be hidden once full.
    pub fn done_adding_work(&self) {
        assert!(
            self.upcoming.swap(false, Ordering::Release),
            "should only need to freeze remaining progress once"
        );
    }
}
