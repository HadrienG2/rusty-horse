//! Ngram usage statistics

use crate::{
    add_nonzero_usize,
    config::Config,
    file::{self, Entry},
    Ngram, Year,
};
use std::{
    cmp::Ordering,
    collections::{hash_map, HashMap},
    num::NonZeroUsize,
    sync::Arc,
};
use unicase::UniCase;

/// Cumulative knowledge from a data file
///
/// Accumulated using [`FileStatsBuilder`]
pub type FileStats = HashMap<UniCase<Ngram>, CaseStats>;

/// Cumulative knowledge from a data file
#[derive(Debug)]
pub struct FileStatsBuilder {
    /// Data collection configuration
    config: Arc<Config>,

    /// Last accepted ngram, if any, and accumulated stats associated with it
    current_ngram_and_stats: Option<(Ngram, NgramStats)>,

    /// Accumulated stats across ngram case equivalence classes
    ///
    /// For each ngram case equivalence class, provides...
    /// - Total stats across the case equivalence class
    /// - Current case with top stats, and stats for this case
    file_stats: FileStats,
}
//
impl FileStatsBuilder {
    /// Set up the accumulator
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            current_ngram_and_stats: None,
            file_stats: FileStats::new(),
        }
    }

    /// Integrate a new dataset entry
    ///
    /// Dataset entries should be added in the order where they come in data
    /// files: sorted by ngram, then by increasing year.
    pub fn add_entry(&mut self, entry: Entry) {
        // If the entry is associated with the current ngram, merge it into the
        // current ngram's statistics
        if let Some((ngram, stats)) = &mut self.current_ngram_and_stats {
            if *ngram == entry.ngram {
                stats.add_year(entry);
                return;
            }
        }

        // Otherwise, flush the current ngram statistics and make the current
        // entry the new current ngram
        self.switch_ngram(Some((entry.ngram.clone(), NgramStats::new(entry))));
    }

    /// Export final statistics at end of dataset processing
    pub fn finish_file(mut self) -> FileStats {
        self.switch_ngram(None);
        self.file_stats
    }

    /// Integrate the current ngram into the file statistics and switch to a
    /// different one (or none at all)
    ///
    /// This should be done when it is established that no other entry for this
    /// ngram will come, either because we just moved to a different ngram
    /// within the data file or because we reached the end of the data file.
    fn switch_ngram(&mut self, new_ngram_and_stats: Option<(Ngram, NgramStats)>) {
        // Update current ngram and get former ngram stats, if any
        if let Some((former_ngram, former_stats)) =
            std::mem::replace(&mut self.current_ngram_and_stats, new_ngram_and_stats)
        {
            // Check if there are sufficient statistics to accept this ngram
            if former_stats.match_count.get() >= self.config.min_matches
                && former_stats.min_volume_count.get() >= self.config.min_books
            {
                // If so, normalize the ngram with non-word rejection...
                let Some(former_ngram) = file::normalizing_filter_map(former_ngram) else {
                    // Ngram does not look like a word, rejecting it...
                    return;
                };

                // ...then record it into the case-insensitive file statistics
                log::trace!("Accepted ngram {former_ngram:?} with {former_stats:?} into current file statistics");
                match self.file_stats.entry(UniCase::new(former_ngram.clone())) {
                    hash_map::Entry::Occupied(o) => {
                        let o = o.into_mut();
                        log::trace!("Merged into existing case-equivalence class {o:#?}");
                        o.add_casing(former_ngram, former_stats);
                        log::trace!("Result of equivalence class merging is {o:#?}");
                    }
                    hash_map::Entry::Vacant(v) => {
                        v.insert(CaseStats::new(former_ngram, former_stats));
                    }
                }
            } else {
                // If the ngram is rejected, log it for posterity
                log::trace!("Rejected ngram {former_ngram:?} with {former_stats:?} from file statistics due to insufficient occurences");
            }
        }
    }
}

/// Cumulative knowledge about an ngram case equivalence class
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaseStats {
    /// Accumulated stats across the entire case equivalence class
    total_stats: NgramStats,

    /// Casing with the best stats
    top_casing: Ngram,

    /// Case-sensitive stats for this casing
    top_stats: NgramStats,
}
//
impl CaseStats {
    /// Set up case-insensitive statistics from case-sensitive ones
    pub fn new(ngram: Ngram, stats: NgramStats) -> Self {
        Self {
            total_stats: stats,
            top_casing: ngram,
            top_stats: stats,
        }
    }

    /// Add a new case-equivalent ngram to these stats
    ///
    /// It is assumed that you have checked that this ngram is indeed
    /// case-equivalent to the one that the stats were created with.
    pub fn add_casing(&mut self, ngram: Ngram, stats: NgramStats) {
        // Sanity checks
        debug_assert!(
            self.total_stats >= self.top_stats,
            "total_stats should integrate the top casing's stats and then some"
        );

        // Add ngram statistics to the case-equivalent statistics
        self.total_stats.merge_cases(stats);

        // If this ngram has better stats than the previous top ngram, it
        // becomes the new top ngram.
        if ngram == self.top_casing {
            self.top_stats.merge_cases(stats);
        } else if stats > self.top_stats {
            self.top_casing = ngram;
            self.top_stats = stats;
        }
    }

    /// Merge case equivalent statistics from another data file into these ones
    ///
    /// You should have checked that the case equivalence class you're merging
    /// is indeed equivalent to this one.
    pub fn merge_files(&mut self, rhs: CaseStats) {
        debug_assert!(
            self.total_stats >= self.top_stats,
            "total_stats should integrate the top casing's stats and then some"
        );
        self.total_stats.merge_cases(rhs.total_stats);
        if rhs.top_casing == self.top_casing {
            self.top_stats.merge_cases(rhs.top_stats);
        } else if rhs.top_stats > self.top_stats {
            self.top_casing = rhs.top_casing;
            self.top_stats = rhs.top_stats;
        }
    }

    /// Extract the top spelling and case-insensitive statistics
    pub fn collect(self) -> (Ngram, NgramStats) {
        (self.top_casing, self.total_stats)
    }
}

/// Cumulative knowledge about an ngram
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct NgramStats {
    /// Year of first occurence
    first_year: Year,

    /// Year of last occurence
    last_year: Year,

    /// Total number of matches over period of interest
    match_count: NonZeroUsize,

    /// Lower bound on the number of books with matches over period of interest
    ///
    /// Is an exact count as long as the stats only cover a single ngram
    /// casing, but becomes a lower bound when equivalent casing are merged.
    min_volume_count: NonZeroUsize,
}
//
impl NgramStats {
    /// Set up statistics from a single dataset entry
    pub fn new(entry: Entry) -> Self {
        Self::validate_entry(&entry);
        Self {
            first_year: entry.year,
            last_year: entry.year,
            match_count: entry.match_count,
            min_volume_count: entry.volume_count,
        }
    }

    /// Update statistics with a new yearly entry
    ///
    /// Yearly entries should be merged in the order that they appear in data
    /// files, i.e. from least recent to most recent.
    pub fn add_year(&mut self, entry: Entry) {
        debug_assert!(
            self.first_year <= self.last_year,
            "Violated first < last year type invariant"
        );
        Self::validate_entry(&entry);
        assert!(
            entry.year > self.first_year.max(self.last_year),
            "Dataset entries should be sorted by year"
        );
        self.last_year = entry.year;
        self.match_count = add_nonzero_usize(self.match_count, entry.match_count);
        // We can add volume counts here because we're still case-sensitive at
        // this point in time and books only have one publication date so
        // different year entries won't refer to the same book.
        self.min_volume_count = add_nonzero_usize(self.min_volume_count, entry.volume_count);
    }

    /// Merge statistics from two case-equivalent ngrams
    ///
    /// You should only do this after you're done accumulating data for the
    /// current ngram
    pub fn merge_cases(&mut self, rhs: NgramStats) {
        self.first_year = self.first_year.min(rhs.first_year);
        self.last_year = self.last_year.max(rhs.last_year);
        self.match_count = add_nonzero_usize(self.match_count, rhs.match_count);
        // We cannot add volume counts here because different casings of the
        // same word may appear within the same book, so we must take a
        // pessimistic lower bound.
        self.min_volume_count = self.min_volume_count.max(rhs.min_volume_count);
    }

    /// Make sure that an entry is reasonable
    fn validate_entry(entry: &Entry) {
        assert!(
            entry.match_count >= entry.volume_count,
            "Entry cannot appear in more books than it has matches"
        );
    }
}
//
impl Ord for NgramStats {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.match_count.cmp(&other.match_count) {
            Ordering::Greater => return Ordering::Greater,
            Ordering::Less => return Ordering::Less,
            Ordering::Equal => {}
        }
        match self.min_volume_count.cmp(&other.min_volume_count) {
            Ordering::Greater => return Ordering::Greater,
            Ordering::Less => return Ordering::Less,
            Ordering::Equal => {}
        }
        match self.last_year.cmp(&other.last_year) {
            Ordering::Greater => return Ordering::Greater,
            Ordering::Less => return Ordering::Less,
            Ordering::Equal => {}
        }
        match self.first_year.cmp(&other.first_year) {
            Ordering::Less => Ordering::Greater,
            Ordering::Greater => Ordering::Less,
            Ordering::Equal => Ordering::Equal,
        }
    }
}
//
impl PartialOrd for NgramStats {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
