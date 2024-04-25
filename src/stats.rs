//! Ngram usage statistics

use crate::{add_nz_u64, config::Config, Year, YearData};
use std::{cmp::Ordering, num::NonZeroU64};

/// Cumulative knowledge about a single ngram or case equivalence class
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct NgramStats {
    /// Year of first occurence
    first_year: Year,

    /// Year of last occurence
    last_year: Year,

    /// Total number of matches over period of interest
    match_count: NonZeroU64,

    /// Lower bound on the number of books with matches over period of interest
    ///
    /// Is an exact count as long as the stats only cover a single ngram, but
    /// becomes a lower bound as soon equivalent ngrams are merged in.
    min_volume_count: NonZeroU64,
}
//
impl NgramStats {
    /// Truth that an ngram with these usage statistics can be accepted with the
    /// current configuration, if other criteria pass
    pub fn is_acceptable(&self, config: &Config) -> bool {
        config.min_matches <= self.match_count
            && config.min_books <= self.min_volume_count
            && config.min_year <= self.last_year
    }

    /// Update statistics with a new yearly entry
    pub fn add_year(&mut self, data: YearData) {
        debug_assert!(
            self.first_year <= self.last_year,
            "Violated first < last year type invariant"
        );
        Self::validate_entry(&data);
        self.first_year = self.first_year.min(data.year);
        self.last_year = self.last_year.max(data.year);
        self.match_count = add_nz_u64(self.match_count, data.match_count);
        // We can add volume counts here because we're still case-sensitive at
        // this point in time and books only have one publication date so
        // different year entries won't refer to the same book.
        self.min_volume_count = add_nz_u64(self.min_volume_count, data.volume_count.into());
    }

    /// Merge statistics from another case- or tag-equivalent ngrams
    ///
    /// You should only do this after you're done accumulating data for the
    /// current ngram
    pub fn merge_equivalent(&mut self, rhs: NgramStats) {
        self.first_year = self.first_year.min(rhs.first_year);
        self.last_year = self.last_year.max(rhs.last_year);
        self.match_count = add_nz_u64(self.match_count, rhs.match_count);
        // We cannot add volume counts here because different casings of the
        // same word may appear within the same book, so we must take a
        // pessimistic lower bound.
        self.min_volume_count = self.min_volume_count.max(rhs.min_volume_count);
    }

    /// Make sure that an entry is reasonable
    fn validate_entry(data: &YearData) {
        assert!(
            data.match_count >= NonZeroU64::from(data.volume_count),
            "Entry cannot appear in more books than it has matches"
        );
    }
}
//
impl From<YearData> for NgramStats {
    fn from(data: YearData) -> Self {
        Self::validate_entry(&data);
        Self {
            first_year: data.year,
            last_year: data.year,
            match_count: data.match_count,
            min_volume_count: data.volume_count.into(),
        }
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
