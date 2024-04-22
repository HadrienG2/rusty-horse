//! Apache Arrow/Parquet disk cache of the Google Ngram dataset
//!
//! The dataset is provided as gzipped TSV, which is a needlessly inefficient
//! format. Instead, our disk cache will use an Apache Parquet based format
//! tailored to our needs :
//!
//! - N-grams are grouped by case equivalence classes
//! - Yearly data does not include unnecessary redundant n-gram copies
//! - Everything is sorted by decreasing year and popularity to enable early
//!   exit when some user cutoff is reached.

use crate::{stats::NgramStats, Ngram, Year};
use std::{
    collections::{BinaryHeap, HashMap, VecDeque},
    num::NonZeroUsize,
};
use unicase::UniCase;

/// Mechanism for collecting the dataset into an ordered Arrow table
pub struct CacheBuilder {
    /// Previously recorded N-gram case equivalence classes
    equivalence_classes: HashMap<UniCase<Ngram>, CaseEquivalenceClassBuilder>,

    /// Last N-gram seen within the file, if any
    current_ngram: Option<Ngram>,

    /// Accumulated stats from accepted entries for current_ngram, if any
    current_stats: Option<CasingBuilder>,
}
//
// TODO: Methods (including conversion to final Apache Arrow format)

/// Mechanism for collecting data about a single case equivalence class
struct CaseEquivalenceClassBuilder {
    /// All-time statistics aggregated across the entire equivalence class
    stats: NgramStats,

    /// Individual casings, sorted by...
    casings: BinaryHeap<(
        // ...decreasing all-time statistics...
        NgramStats,
        // ...then spelling (which will always disambiguate)
        Ngram,
        // Finally, we keep per-year information, sorted by decreasing year
        Box<[YearData]>,
    )>,
}

/// Mechanism for collecting data bout a single casing
struct CasingBuilder {
    /// Statistics aggregated over all years seen so far
    stats: NgramStats,

    /// Yearly data aggregated so far
    ///
    /// Years will come in increasing order, and should be inserted using
    /// push_front. This way, front-to-back iteration will yield data by
    /// decreasing year, which is the desired final storage order.
    years: VecDeque<YearData>,
}

/// Data about a single N-gram for a single year
struct YearData {
    /// Year on which that data was recorded
    year: Year,

    /// Number of recorded occurences
    occurences: NonZeroUsize,

    /// Number of books across which occurences were recorded
    books: NonZeroUsize,
}
