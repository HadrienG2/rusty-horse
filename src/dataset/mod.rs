//! Reorganization of the Google dataset to better fit our processing needs :
//!
//! - Ngrams are grouped by case equivalence classes
//! - Yearly data does not include unnecessary redundant ngram copies
//! - Everything is sorted by decreasing popularity/year to enable early exit
//!   when some user cutoff is reached.

// FIXME: Add users, then remove this
#![allow(unused)]

pub mod builder;

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

/// Cumulative knowledge aggregated from files of the Google Books dataset
///
/// Sliced into independent blocks for easy parallelism and asynchronism.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Dataset(Box<[DatasetBlock]>);
//
impl Dataset {
    /// Iterate sequentially over all case classes in the dataset
    pub fn case_classes(&self) -> impl Iterator<Item = CaseClassView<'_>> {
        self.blocks().iter().flat_map(DatasetBlock::case_classes)
    }

    /// Access the dataset in a block-wise fashion
    pub fn blocks(&self) -> &[DatasetBlock] {
        &self.0[..]
    }
}

/// Block of data from a [`Dataset`]
///
/// Case equivalence classes and ngrams are sorted by decreasing all-time usage
/// frequency, and for each ngram, data columns are sorted by decreasing year.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DatasetBlock {
    /// Offsets that mark the end of each case equivalence class in the
    /// "ngram_str_ends" and "ngram_data_ends" arrays
    case_class_ends: Box<[usize]>,

    /// Offsets that mark the end of each ngram in the "ngrams" string
    ngram_str_ends: Box<[usize]>,

    /// Concatenated ngrams from all equivalence classes
    ngrams: Box<str>,

    /// Offsets that mark the end of the yearly data from each ngram in the
    /// "years", "match_counts" and "volume_counts" arrays
    ngram_data_ends: Box<[usize]>,

    /// Concatenated "year" data columns from all ngrams
    years: Box<[Year]>,

    /// Concatenated "match_count" data columns from all ngrams
    match_counts: Box<[YearMatchCount]>,

    /// Concatenated "volume_count" data columns from all ngrams
    volume_counts: Box<[YearVolumeCount]>,
}
//
impl DatasetBlock {
    /// Iterate over ngram case equivalence classes
    ///
    /// Case equivalence classes will be enumerated in decreasing all-time
    /// popularity order.
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
    dataset: &'dataset DatasetBlock,

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
