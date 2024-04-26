//! Mechanism for building a [`Dataset`] from the Google Books Ngram input data

use super::{Dataset, DatasetBlock};
use crate::{
    add_nz_u64,
    config::Config,
    progress::{ProgressConfig, ProgressReport, Work},
    stats::NgramStats,
    tsv::{self, Entry},
    Ngram, Year, YearData, YearMatchCount, YearVolumeCount,
};
use rayon::prelude::*;
use std::{
    cmp::{Ordering, Reverse},
    collections::{hash_map, HashMap, VecDeque},
    sync::Arc,
};
use unicase::UniCase;

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
        let entry_data = entry.data();
        if let Some((ngram, data)) = &mut self.current_ngram_and_data {
            if *ngram == entry.ngram {
                data.add_year(entry_data);
                return;
            }
        }

        // Otherwise, flush the current ngram data and make the current entry
        // the new current ngram
        self.switch_ngram(Some((entry.ngram, NgramDataBuilder::from(entry_data))));
    }

    /// Export the file dataset
    ///
    /// Call this when you're done accumulating entries from a source data file.
    pub fn finish_file(mut self) -> DatasetFiles {
        self.switch_ngram(None);
        DatasetFiles {
            config: self.config,
            data: self.equivalence_classes,
        }
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
            if former_data.stats.is_acceptable(&self.config) {
                // If so, normalize the ngram with non-word rejection...
                let Some(former_ngram) = tsv::normalizing_filter_map(former_ngram) else {
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
/// input file. Can be used to aggregate data from other input data files, then
/// turned into a [`Dataset`] once done.
#[derive(Debug)]
pub struct DatasetFiles {
    /// Data collection configuration
    config: Arc<Config>,

    /// Accumulated data
    data: HashMap<UniCase<Ngram>, CaseEquivalentData>,
}
//
impl DatasetFiles {
    /// Create an empty accumulator
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            data: HashMap::new(),
        }
    }

    /// Merge with data from another file
    pub fn merge(&mut self, other: Self) {
        for (name, stats) in other.data {
            match self.data.entry(name) {
                hash_map::Entry::Occupied(o) => o.into_mut().merge_equivalent(stats),
                hash_map::Entry::Vacant(v) => {
                    v.insert(stats);
                }
            }
        }
    }

    /// Convert the dataset to its final form
    pub fn finish(self, report: &ProgressReport) -> Arc<Dataset> {
        // Order case equivalence classes by decreasing stats...
        let sort = report.add(
            "Sorting ngrams by usage",
            ProgressConfig::new(Work::PercentSteps(self.data.len())),
        );
        let mut case_classes = (self.data.into_par_iter())
            .map(|(_key, class)| {
                // ...and within each class, order ngrams by decreasing stats
                // Yearly data for each ngram was already ordered by decreasing
                // year during construction, so no reordering is needed here.
                let stats = class.stats;
                let mut casings = class.casings.into_iter().collect::<Vec<_>>();
                casings.sort_unstable_by_key(|(_ngram, data)| Reverse(data.stats));
                sort.make_progress(1);
                (stats, casings)
            })
            .collect::<Vec<_>>();
        case_classes.par_sort_unstable_by_key(|(stats, _casings)| Reverse(*stats));

        // Convert the ordered data into the final dataset layout
        let build = report.add(
            "Optimizing data layout",
            ProgressConfig::new(Work::PercentSteps(case_classes.len())),
        );
        let dataset_blocks = case_classes
            .par_chunks(self.config.memory_chunk.get())
            .map(|chunk| {
                let mut builder = DatasetBlockBuilder::new();
                for (_stats, casings) in chunk {
                    let mut case_class = builder.new_case_class();
                    for (ngram, data) in casings {
                        case_class.push(ngram, data.years.iter().copied())
                    }
                    build.make_progress(1);
                }
                builder.build()
            })
            .collect::<_>();

        // Parallelize the liberation of the original data and offload it to a
        // background thread, as this is a surprisingly expensive operation!
        std::thread::spawn(move || case_classes.into_par_iter().for_each(std::mem::drop));
        Arc::new(Dataset(dataset_blocks))
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

/// Accumulator of data in the final dataset layout
///
/// All fields have the same meaning as in [`DatasetBlock`].
#[derive(Debug, Default)]
struct DatasetBlockBuilder {
    case_class_ends: Vec<usize>,
    ngram_str_ends: Vec<usize>,
    ngrams: String,
    ngram_data_ends: Vec<usize>,
    years: Vec<Year>,
    match_counts: Vec<YearMatchCount>,
    volume_counts: Vec<YearVolumeCount>,
}
//
impl DatasetBlockBuilder {
    /// Create a new dataset block
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a new case equivalence class to the block
    pub fn new_case_class(&mut self) -> CaseClassBuilder<'_> {
        CaseClassBuilder(self)
    }

    /// Build the final dataset block
    pub fn build(self) -> DatasetBlock {
        DatasetBlock {
            case_class_ends: self.case_class_ends.into(),
            ngram_str_ends: self.ngram_str_ends.into(),
            ngrams: self.ngrams.into(),
            ngram_data_ends: self.ngram_data_ends.into(),
            years: self.years.into(),
            match_counts: self.match_counts.into(),
            volume_counts: self.volume_counts.into(),
        }
    }
}
//
impl From<DatasetBlockBuilder> for DatasetBlock {
    fn from(value: DatasetBlockBuilder) -> Self {
        value.build()
    }
}
//
/// Proxy object used to insert a new case equivalence class in a dataset block
#[derive(Debug)]
struct CaseClassBuilder<'block>(&'block mut DatasetBlockBuilder);
//
impl CaseClassBuilder<'_> {
    /// Add a case-equivalent ngram to this case equivalence class
    ///
    /// Ngrams should be added in order of decreasing popularity, and yearly
    /// data should be specified in order of decreasing data.
    fn push(&mut self, ngram: &str, year_data: impl IntoIterator<Item = YearData>) {
        self.0.ngrams.push_str(ngram);
        self.0.ngram_str_ends.push(self.0.ngrams.len());
        for YearData {
            year,
            match_count,
            volume_count,
        } in year_data
        {
            self.0.years.push(year);
            self.0.match_counts.push(match_count);
            self.0.volume_counts.push(volume_count);
        }
        self.0.ngram_data_ends.push(self.0.volume_counts.len());
    }
}
//
impl Drop for CaseClassBuilder<'_> {
    fn drop(&mut self) {
        self.0.case_class_ends.push(self.0.ngram_data_ends.len());
    }
}
