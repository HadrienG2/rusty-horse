//! Reorganization of the Google dataset to better fit our processing needs :
//!
//! - Ngrams are grouped by case equivalence classes
//! - Yearly data does not include unnecessary redundant ngram copies
//! - Everything is sorted by decreasing popularity/year to enable early exit
//!   when the user cutoff is reached.

// FIXME: Add users, then remove this
#![allow(unused)]

use crate::{
    add_nz_u64,
    config::Config,
    file::{self, Entry, YearData},
    stats::NgramStats,
    Ngram, Year, YearMatchCount, YearVolumeCount,
};
use rayon::prelude::*;
use std::{
    cmp::{Ordering, Reverse},
    collections::{hash_map, BTreeMap, BinaryHeap, HashMap, VecDeque},
    sync::Arc,
};
use unicase::UniCase;

/// Cumulative knowledge aggregated from all files of the Google Books dataset
///
/// Case equivalence classes and ngrams are sorted by decreasing all-time usage
/// frequency, and for each ngram, data columns are sorted by decreasing year.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Dataset {
    /// Offsets that mark the end of each case equivalence class in the
    /// "ngram_str_ends" and "ngram_data_ends" arrays
    case_class_ends: Box<[usize]>,

    /// Offsets that mark the end of each ngram in the "ngrams" string
    //
    // TODO: Try delta encoding to shrink dataset size
    ngram_str_ends: Box<[usize]>,

    /// Concatenated ngrams from all equivalence classes
    ngrams: Box<str>,

    /// Offsets that mark the end of the yearly data from each ngram in the
    /// "years", "match_counts" and "volume_counts" arrays
    //
    // TODO: Try delta encoding to shrink dataset size
    ngram_data_ends: Box<[usize]>,

    /// Concatenated "year" data columns from all ngrams
    years: Box<[Year]>,

    /// Concatenated "match_count" data columns from all ngrams
    match_counts: Box<[YearMatchCount]>,

    /// Concatenated "volume_count" data columns from all ngrams
    volume_counts: Box<[YearVolumeCount]>,
}
//
impl Dataset {
    /// Iterate over ngram case equivalence classes
    ///
    /// Case equivalence classes will be enumerated in decreasing all-time
    /// popularity order.
    //
    // TODO: Make this amenable to parallel iteration over case classes
    pub fn case_classes(&self) -> impl Iterator<Item = CaseClassView<'_>> {
        let mut last_class_end = 0;
        let mut last_str_end = 0;
        let mut last_data_end = 0;
        (self.case_class_ends.iter().copied()).map(move |case_class_end| {
            // Extract relevant data range for this case equivalence class
            let case_class_range = last_class_end..case_class_end;
            let ngram_str_ends = &self.ngram_str_ends[case_class_range.clone()];
            let ngram_data_ends = &self.ngram_data_ends[case_class_range];
            let result = CaseClassView {
                dataset: self,
                ngrams_str_start: last_str_end,
                ngram_str_ends,
                ngram_data_start: last_data_end,
                ngram_data_ends,
            };

            // Update state variables for next equivalence class
            last_class_end = case_class_end;
            last_str_end = *(ngram_str_ends.last()).expect("Case classes should have ngrams");
            last_data_end = *(ngram_data_ends.last()).expect("Case classes should have data");
            result
        })
    }
}
//
/// Case equivalence class from the dataset
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CaseClassView<'dataset> {
    /// Source dataset
    dataset: &'dataset Dataset,

    /// Offset of the start of this case class in the "ngrams" string
    ngrams_str_start: usize,

    /// Offsets of the end of each ngram in the "ngrams" string
    ngram_str_ends: &'dataset [usize],

    /// Offset of the start of this case class in the data arrays
    ngram_data_start: usize,

    /// Offsets of the end of each ngram in the data arrays
    ngram_data_ends: &'dataset [usize],
}
//
impl<'dataset> CaseClassView<'dataset> {
    /// Iterate over ngrams within this case class
    pub fn ngrams(self) -> impl Iterator<Item = NgramView<'dataset>> {
        let mut last_str_end = 0;
        let mut last_data_end = 0;
        (self.ngram_str_ends.iter().copied())
            .zip(self.ngram_data_ends.iter().copied())
            .map(move |(str_end, data_end)| {
                // Extract relevant data range for this ngram
                let ngram = &self.dataset.ngrams[last_str_end..str_end];
                let data_range = last_data_end..data_end;
                let result = NgramView {
                    ngram,
                    years: &self.dataset.years[data_range.clone()],
                    match_counts: &self.dataset.match_counts[data_range.clone()],
                    volume_counts: &self.dataset.volume_counts[data_range],
                };

                // Update state variables for next ngram
                last_str_end = str_end;
                last_data_end = data_end;
                result
            })
    }
}
//
/// Ngram from the dataset
pub struct NgramView<'dataset> {
    /// Text of this ngram
    ngram: &'dataset str,

    /// Years where this ngram was seen in books (sorted in decreasing order)
    years: &'dataset [Year],

    /// Number of matches on each year across all books published that year
    match_counts: &'dataset [YearMatchCount],

    /// Number of books with matches on each year
    volume_counts: &'dataset [YearVolumeCount],
}
//
impl<'dataset> NgramView<'dataset> {
    /// Text of this ngram
    pub fn ngram(&self) -> &'dataset str {
        self.ngram
    }

    /// Yearly data for this ngram, sorted by decreasing year
    pub fn years(&self) -> impl Iterator<Item = YearData> + 'dataset {
        (self.years.iter())
            .zip(self.match_counts)
            .zip(self.volume_counts)
            .map(|((&year, &match_count), &volume_count)| YearData {
                year,
                match_count,
                volume_count,
            })
    }
}

/// Accumulator for entries from a single file of the Google Books Ngram dataset
///
/// Once you're done with an input file, call
/// [`finish_file()`](Self::finish_file) to get a simplified [`DatasetFiles`]
/// accumulator that can only merge data from different input files.
#[derive(Debug)]
pub struct DatasetBuilder {
    /// Data collection configuration
    config: Arc<Config>,

    /// Last seen ngram, if any, and accumulated data about it
    current_ngram_and_data: Option<(Ngram, NgramDataBuilder)>,

    /// Data from previous ngrams, grouped by case equivalence classes
    equivalence_classes: HashMap<UniCase<Ngram>, CaseEquivalentData>,
}
//
impl DatasetBuilder {
    /// Set up the accumulator
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            current_ngram_and_data: None,
            equivalence_classes: HashMap::new(),
        }
    }

    /// Integrate a new dataset entry
    ///
    /// Dataset entries should be added in the order where they come in data
    /// files: sorted by ngram, then by increasing year.
    pub fn add_entry(&mut self, entry: Entry) {
        // If the entry is associated with the current ngram, merge it into the
        // current ngram's statistics
        if let Some((ngram, data)) = &mut self.current_ngram_and_data {
            if *ngram == entry.ngram {
                data.add_year(entry.data);
                return;
            }
        }

        // Otherwise, flush the current ngram data and make the current entry
        // the new current ngram
        self.switch_ngram(Some((entry.ngram, NgramDataBuilder::from(entry.data))));
    }

    /// Export the file dataset
    ///
    /// Call this when you're done accumulating entries from a source data file.
    pub fn finish_file(mut self) -> DatasetFiles {
        self.switch_ngram(None);
        DatasetFiles(self.equivalence_classes)
    }

    /// Integrate the current ngram into the dataset and switch to a different
    /// ngram (or none at all)
    ///
    /// This should be done when it is established that no other entry for this
    /// ngram will come, either because we just moved to a different ngram
    /// within the source data file or because we reached the end of that file.
    fn switch_ngram(&mut self, new_ngram_and_data: Option<(Ngram, NgramDataBuilder)>) {
        // Update current ngram and get former ngram data, if any
        if let Some((former_ngram, former_data)) =
            std::mem::replace(&mut self.current_ngram_and_data, new_ngram_and_data)
        {
            // Check if there are sufficient statistics to accept this ngram
            if former_data.stats.match_count() >= self.config.min_matches
                && former_data.stats.min_volume_count() >= self.config.min_books
            {
                // If so, normalize the ngram with non-word rejection...
                let Some(former_ngram) = file::normalizing_filter_map(former_ngram) else {
                    // Ngram does not look like a word, rejecting it...
                    return;
                };

                // ...then record it into the case-insensitive file statistics
                log::trace!(
                    "Accepted ngram {former_ngram:?} with {former_data:?} into the dataset"
                );
                let former_data = former_data.build();
                match self
                    .equivalence_classes
                    .entry(UniCase::new(former_ngram.clone()))
                {
                    hash_map::Entry::Occupied(o) => {
                        let o = o.into_mut();
                        log::trace!("Merged into existing case-equivalence class {o:#?}");
                        o.add_casing(former_ngram, former_data);
                        log::trace!("Result of equivalence class merging is {o:#?}");
                    }
                    hash_map::Entry::Vacant(v) => {
                        v.insert(CaseEquivalentData::new(former_ngram, former_data));
                    }
                }
            } else {
                // If the ngram is rejected, log it for posterity
                log::trace!("Rejected ngram {former_ngram:?} with {former_data:?} from the dataset due to insufficient occurences");
            }
        }
    }
}
//
impl From<DatasetBuilder> for DatasetFiles {
    fn from(value: DatasetBuilder) -> Self {
        value.finish_file()
    }
}

/// Accumulated knowledge from one or more input data files
///
/// Produced from [`DatasetBuilder`] once done accumulating data about a single
/// input file. Can be used to aggregate data from all input data files, then
/// turned into a [`Dataset`] once done.
pub struct DatasetFiles(HashMap<UniCase<Ngram>, CaseEquivalentData>);
//
impl DatasetFiles {
    /// Merge with data from another file
    pub fn merge(&mut self, other: Self) {
        for (name, stats) in other.0 {
            match self.0.entry(name) {
                hash_map::Entry::Occupied(o) => o.into_mut().merge_equivalent(stats),
                hash_map::Entry::Vacant(v) => {
                    v.insert(stats);
                }
            }
        }
    }

    /// Convert the dataset to its final form
    pub fn finish(self) -> Dataset {
        // Order case equivalence classes by decreasing stats, then by
        // lexicographic order
        let ordered_case_classes = (self.0.into_par_iter())
            .map(|(ngram, case)| {
                // Order case-equivalent ngrams similarly within each class
                let ordered_casings = (case.casings.into_iter())
                    .map(|(ngram, data)| {
                        // Yearly data was already ordered as desired (by
                        // decreasing year) during construction, so it can be
                        // passed down as is.
                        let order_key = (Reverse(case.stats), ngram);
                        (order_key, data.years)
                    })
                    .collect::<BTreeMap<_, _>>();
                let order_key = (Reverse(case.stats), ngram.into_inner());
                (order_key, ordered_casings)
            })
            .collect::<BTreeMap<_, _>>();

        // Collect data into the final dataset layout
        //
        // NOTE: If this becomes a sequential bottleneck, it can be parallelized
        //       by switching the dataset to a block-based format.
        let mut case_class_ends = Vec::new();
        let mut ngrams_str_ends = Vec::new();
        let mut ngrams = String::new();
        let mut ngrams_data_ends = Vec::new();
        let mut years = Vec::new();
        let mut match_counts = Vec::new();
        let mut volume_counts = Vec::new();
        for ordered_casings in ordered_case_classes.into_values() {
            for ((_stats, ngram), years_data) in ordered_casings {
                ngrams.push_str(&ngram);
                ngrams_str_ends.push(ngrams.len());
                for YearData {
                    year,
                    match_count,
                    volume_count,
                } in years_data.into_vec()
                {
                    years.push(year);
                    match_counts.push(match_count);
                    volume_counts.push(volume_count);
                }
                ngrams_data_ends.push(years.len());
            }
            case_class_ends.push(ngrams_str_ends.len());
        }
        Dataset {
            case_class_ends: case_class_ends.into(),
            ngram_str_ends: ngrams_str_ends.into(),
            ngrams: ngrams.into(),
            ngram_data_ends: ngrams_data_ends.into(),
            years: years.into(),
            match_counts: match_counts.into(),
            volume_counts: volume_counts.into(),
        }
    }
}
//
impl From<DatasetFiles> for Dataset {
    fn from(value: DatasetFiles) -> Self {
        value.finish()
    }
}

/// Accumulator for data about a single ngram case equivalence class
#[derive(Clone, Debug, Eq, PartialEq)]
struct CaseEquivalentData {
    /// All-time statistics aggregated across the entire equivalence class
    stats: NgramStats,

    /// Case-equivalent ngrams
    casings: HashMap<Ngram, NgramData>,
}
//
impl CaseEquivalentData {
    /// Create a case equivalence class from a single ngram
    pub fn new(ngram: Ngram, data: NgramData) -> Self {
        Self {
            stats: data.stats,
            casings: std::iter::once((ngram, data)).collect(),
        }
    }

    /// Add a new case-equivalent ngram
    pub fn add_casing(&mut self, ngram: Ngram, data: NgramData) {
        // Add ngram statistics to the case-equivalent statistics
        self.stats.merge_equivalent(data.stats);

        // Record or update per-ngram information
        self.merge_ngram(ngram, data);
    }

    /// Merge this data with other case-equivalent data
    pub fn merge_equivalent(&mut self, other: Self) {
        // Merge statistics
        self.stats.merge_equivalent(other.stats);

        // Merge per-ngram information
        for (ngram, data) in other.casings {
            self.merge_ngram(ngram, data);
        }
    }

    /// Merge per-ngram information without updating global stats
    fn merge_ngram(&mut self, ngram: Ngram, data: NgramData) {
        match self.casings.entry(ngram) {
            hash_map::Entry::Occupied(mut o) => o.get_mut().merge_equivalent(data),
            hash_map::Entry::Vacant(v) => {
                v.insert(data);
            }
        }
    }
}

/// Accumulated data about a single (case-sensitive) ngram
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct NgramData {
    /// Statistics aggregated over all years of occurence
    stats: NgramStats,

    /// Raw yearly data, sorted by decreasing year
    years: Box<[YearData]>,
}
//
impl NgramData {
    /// Merge data from another identical ngram, with the same casing
    ///
    /// Identical ngrams typically appear as a result of normalization
    /// operations, including removal of grammar tags or decomposition of ngrams
    /// that are internally composed of multiple independent words.
    fn merge_equivalent(&mut self, other: Self) {
        // First, merge in the statistical data
        self.stats.merge_equivalent(other.stats);

        // Then merge the yearly data
        let mut years = Vec::with_capacity(self.years.len().max(other.years.len()));
        let mut lhs = self.years.iter().copied().peekable();
        let mut rhs = other.years.iter().copied().peekable();
        // While there's yearly data from both sides, merge these sorted lists
        while let (Some(l), Some(r)) = (lhs.peek().copied(), rhs.peek().copied()) {
            years.push(match l.year.cmp(&r.year) {
                // If we get entries from the same year on both sides, merge
                // them using the same logic as NgramStats
                Ordering::Equal => {
                    lhs.next();
                    rhs.next();
                    YearData {
                        year: l.year,
                        // Can safely merge match counts from two entries...
                        match_count: add_nz_u64(l.match_count, r.match_count),
                        // ...but can't add volume counts because equivalent
                        // spellings of an ngram could appear in the same book
                        volume_count: l.volume_count.max(r.volume_count),
                    }
                }
                // Otherwise, pick the most recent entry
                Ordering::Greater => {
                    lhs.next();
                    l
                }
                Ordering::Less => {
                    rhs.next();
                    r
                }
            });
        }
        // Once data only remains on one side, we can just flush it
        years.extend(lhs.chain(rhs));
        self.years = years.into();
    }
}

/// Accumulator for yearly data from a single (case-sensitive) ngram
#[derive(Debug)]
struct NgramDataBuilder {
    /// Yearly data aggregated so far
    ///
    /// Years will come in increasing order, and should be inserted using
    /// push_front. This way, front-to-back iteration will yield data by
    /// decreasing year, which is the desired final storage order.
    years: VecDeque<YearData>,

    /// Statistics aggregated over all years seen so far
    stats: NgramStats,
}
//
impl NgramDataBuilder {
    /// Record a new yearly entry about this ngram
    ///
    /// Yearly entries should be merged in the order that they appear in data
    /// files, i.e. from least recent to most recent.
    fn add_year(&mut self, data: YearData) {
        self.stats.add_year(data);
        self.years.push_front(data);
    }

    /// Finalize the data once we're sure no more yearly data is coming
    fn build(self) -> NgramData {
        NgramData {
            stats: self.stats,
            years: Vec::from(self.years).into_boxed_slice(),
        }
    }
}
//
impl From<YearData> for NgramDataBuilder {
    fn from(data: YearData) -> Self {
        Self {
            stats: NgramStats::from(data),
            years: std::iter::once(data).collect(),
        }
    }
}
//
impl From<NgramDataBuilder> for NgramData {
    fn from(value: NgramDataBuilder) -> Self {
        value.build()
    }
}
