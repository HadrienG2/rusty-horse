//! Progress reporting infrastructure

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;

/// Command line interface state
pub struct ProgressReport {
    /// Set of progress bars displayed by the application
    multi: MultiProgress,

    /// Progress bar indicating how many data files have not started downloading
    start: ProgressBar,

    /// Progress bar indicating the status of running downloads
    bytes: ProgressBar,

    /// Progress bar indicating the status of sorting ngrams
    sort: ProgressBar,
}
//
impl ProgressReport {
    /// Set up the command line interface
    pub fn new(num_data_files: usize) -> Arc<Self> {
        // Set up the overall reporting cli
        let multi = MultiProgress::new();

        // Set up download startup tracking
        let start = ProgressBar::new(num_data_files as _)
            .with_prefix("Initiating data file downloads")
            .with_style(ProgressStyle::with_template("{prefix} {wide_bar} {pos}/{len}").unwrap());
        multi.add(start.clone());

        // Prepare to track ongoing downloads
        let bytes = ProgressBar::new(0)
            .with_prefix("Downloading and processing data")
            .with_style(
                ProgressStyle::with_template(
                    "{prefix} {wide_bar} {bytes}/{total_bytes} ({bytes_per_sec})",
                )
                .unwrap(),
            );

        // Prepare to track ngram sorting
        let sort = ProgressBar::new(0)
            .with_prefix("Picking the top ngrams")
            .with_style(ProgressStyle::with_template("{prefix} {wide_bar} {pos}/{len}").unwrap());
        Arc::new(Self {
            multi,
            start,
            bytes,
            sort,
        })
    }

    /// Record that a download has started
    pub fn start_download(&self, file_size: u64) {
        // Record that a new file is being downloaded
        self.start.inc(1);
        let started_files = self.start.position();
        let num_files = self.start.length().unwrap_or(0);
        assert!(
            started_files <= num_files,
            "started downloading more files than expected"
        );

        // Update counter of remaining bytes, show it if this is the first file
        if started_files == 1 {
            self.multi.add(self.bytes.clone());
        }
        self.bytes.inc_length(file_size);

        // If all expected downloads have started, hide progress bar
        if started_files == num_files {
            self.start.finish_and_clear();
            self.multi.remove(&self.start);
        }
    }

    /// Record that some active donwload has fetched more bytes
    pub fn inc_bytes(&self, num_bytes: usize) {
        // Make sure some downloads were supposed to be running
        assert!(
            self.start.position() > 0,
            "recorded downloaded bytes even though no download was started"
        );

        // Track newly downloaded bytes
        let curr_len = self.bytes.length().unwrap_or(0);
        assert!(
            (curr_len - self.bytes.position())
                .checked_sub(num_bytes as _)
                .is_some(),
            "recorded more downloaded bytes than expected"
        );
        self.bytes.inc(num_bytes as _);

        // Track overall end of downloads
        if self.bytes.position() == curr_len && self.start.is_finished() {
            self.bytes.finish_and_clear();
            self.multi.remove(&self.bytes);
        }
    }

    /// Start tracking the final ngrams sort
    pub fn start_sort(&self, num_ngrams: usize) {
        // Make sure that we are ready to sort
        assert!(
            self.start.is_finished() && self.bytes.is_finished(),
            "should not start sorting ngrams before they have all been collected"
        );

        // Make sure that we have not sorted already
        assert_eq!(
            self.sort.length(),
            Some(0),
            "should only start sorting once"
        );

        // Prepare to track the sort
        self.sort.inc_length(num_ngrams as _);
        self.multi.add(self.sort.clone());
    }

    /// Track that some ngrams have been sorted
    pub fn inc_sorted(&self, num_ngrams: usize) {
        // Make sure we don't overflow the sorting counter
        let curr_len = self.sort.length().unwrap_or(0);
        assert!(
            (curr_len - self.sort.position())
                .checked_sub(num_ngrams as _)
                .is_some(),
            "attempted to sort more than expected"
        );
        assert!(
            !self.sort.is_finished(),
            "should not keep sorting after declaring sorting finished"
        );

        // Track the ongoing sort
        self.sort.inc(num_ngrams as _);
        if self.sort.position() == curr_len {
            self.sort.finish_and_clear();
            self.multi.remove(&self.sort);
        }
    }
}
