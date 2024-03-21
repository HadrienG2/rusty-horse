//! This program is based on the Google Books Ngrams dataset, whose general
//! documentation you can find at
//! <http://storage.googleapis.com/books/ngrams/books/datasetsv3.html>.

mod languages;
mod progress;

use crate::progress::ProgressReport;
use anyhow::Context;
use async_compression::tokio::bufread::GzipDecoder;
use clap::Parser;
use csv_async::AsyncReaderBuilder;
use futures::stream::StreamExt;
use log::LevelFilter;
use reqwest::Response;
use serde::Deserialize;
use std::{
    cmp::{Ordering, Reverse},
    collections::{hash_map, BinaryHeap, HashMap},
    io::{self, ErrorKind},
    num::NonZeroUsize,
    sync::Arc,
};
use tokio::task::JoinSet;
use tokio_util::io::StreamReader;
use unicase::UniCase;

/// Year where the dataset that we use was published
const DATASET_PUBLICATION_YEAR: Year = 2012;

/// TODO: User-visible program description
///
/// All occurence count cutoffs are applied to the total occurence count over
/// the time period of interest.
#[derive(Parser, Debug)]
pub struct Args {
    /// Short name of the Google Ngrams language to be used, e.g. "eng-fiction"
    ///
    /// Will interactively prompt for a supported language if not specified.
    #[arg(short, long, default_value = None)]
    language: Option<Box<str>>,

    // TODO: "Bad word" exclusion mechanism
    /// Minimum accepted book publication year
    ///
    /// Our data set is based on books, many of which have been published a long
    /// time ago and may thus not represent modern language usage. You can
    /// compensate for this bias by ignoring books published before a certain
    /// year, at the cost of reducing the size of the dataset.
    ///
    /// By default, we include books starting 50 years before the date where the
    /// Ngrams dataset was published.
    #[arg(short = 'y', long, default_value = None)]
    min_year: Option<Year>,

    /// Minimum accepted number of matches across of books
    ///
    /// Extremely rare words are not significantly more memorable than random
    /// characters. Therefore we ignore words which occur extremely rarely in
    /// the selected dataset section.
    //
    // TODO: Fine-tune default based on empirically observed irrelevance
    //       threshold
    #[arg(short = 'm', long, default_value_t = 100)]
    min_matches: usize,

    /// Minimum accepted number of matching books
    ///
    /// If a word only appears in a book or two, it may be a neologism from the
    /// author, or the product of an error in the book -> text conversion
    /// process. Therefore, we only consider words which appear across a
    /// sufficiently large number of books.
    //
    // TODO: Fine-tune default based on empirically observed irrelevance
    //       threshold
    #[arg(short = 'b', long, default_value_t = 10)]
    min_books: usize,

    /// Max number of output n-grams
    ///
    /// While it is possible to compute the full list of valid n-grams and trim
    /// it to the desired length later on, knowing the desired number of matches
    /// right from the start allows this program to discard less frequent
    /// n-grams before the full list of n-grams is available. As a result, the
    /// processing will consume less memory and run a little faster.
    #[arg(short = 'n', long, default_value = None)]
    max_outputs: Option<NonZeroUsize>,
}
//
impl Args {
    /// Minimal book publication year cutoff
    pub fn min_year(&self) -> Year {
        self.min_year.unwrap_or(DATASET_PUBLICATION_YEAR - 50)
    }
}

/// Use anyhow for Result type erasure
pub use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Set up logging
    syslog::init(
        syslog::Facility::LOG_USER,
        if cfg!(feature = "log-trace") {
            LevelFilter::Trace
        } else if cfg!(debug_assertions) {
            LevelFilter::Debug
        } else {
            LevelFilter::Info
        },
        None,
    )
    .map_err(|e| anyhow::format_err!("{e}"))?;

    // Decode CLI arguments
    let args = Arc::new(Args::parse());

    // Check CLI arguments for basic sanity
    if let Some(min_year) = args.min_year {
        anyhow::ensure!(
            min_year <= DATASET_PUBLICATION_YEAR,
            "requested minimum publication year excludes all books from the dataset"
        );
    }

    // Let the user pick a language
    let language = if let Some(language) = &args.language {
        languages::get(language)?
    } else {
        languages::prompt()?
    };
    let dataset_urls = language.dataset_urls().collect::<Vec<_>>();

    // TODO: Consider adding some caching so that we don't have to re-download
    //       everything every time

    // Set up progress reporting
    let report = ProgressReport::new(dataset_urls.len());

    // Start all the data file downloading and processing
    let client = reqwest::Client::new();
    let mut data_files = JoinSet::new();
    for url in dataset_urls {
        data_files.spawn(download_and_process(
            args.clone(),
            client.clone(),
            url,
            report.clone(),
        ));
    }

    // Collect and merge statistics from data files as downloads finish
    let mut full_stats = HashMap::<_, CaseStats>::new();
    while let Some(join_result) = data_files.join_next().await {
        for (name, stats) in join_result?? {
            match full_stats.entry(name) {
                hash_map::Entry::Occupied(o) => o.into_mut().merge(stats),
                hash_map::Entry::Vacant(v) => { v.insert(stats); }
            }
        }
    }

    // Sort n-grams by frequency and pick the most frequent ones if requested
    report.start_sort(full_stats.len());
    let mut top_entries = if let Some(max_outputs) = args.max_outputs {
        BinaryHeap::with_capacity(max_outputs.get())
    } else {
        BinaryHeap::new()
    };
    for (_, case_stats) in full_stats.drain() {
        top_entries.push((Reverse(case_stats.total_stats), case_stats.top_ngram));
        if let Some(max_outputs) = args.max_outputs {
            if top_entries.len() > max_outputs.get() {
                top_entries.pop();
            }
        }
        // NOTE: If these atomic increments become too expensive, use
        //       batched iteration to amortize them
        report.inc_sorted(1);
    }

    // Reorder the results by decreasing frequency for final display
    let mut ngrams_by_decreasing_stats = Vec::with_capacity(top_entries.len());
    while let Some((rev_stats, ngram)) = top_entries.pop() {
        ngrams_by_decreasing_stats.push((ngram, rev_stats.0));
    }

    // TODO: Do something sensible with output
    println!("{ngrams_by_decreasing_stats:#?}");

    Ok(())
}

/// Start downloading and processing a data file
async fn download_and_process(
    args: Arc<Args>,
    client: reqwest::Client,
    url: Box<str>,
    report: Arc<ProgressReport>,
) -> Result<FileStats> {
    // Start the download
    let context = || format!("initiating download of {url}");
    let response = client
        .get(&*url)
        .send()
        .await
        .and_then(Response::error_for_status)
        .with_context(context)?;
    report.start_download(response.content_length().with_context(context)?);

    // Process the byte stream into deserialized TSV records
    let context = || format!("fetching and processing {url}");
    let gz_bytes = StreamReader::new(response.bytes_stream().map(|res| {
        res
            // Track how many input bytes have been downloaded so far
            .inspect(|bytes_block| report.inc_bytes(bytes_block.len()))
            // Translate reqwest errors into I/O errors
            .map_err(|e| io::Error::new(ErrorKind::Other, Box::new(e)))
    }));
    let tsv_bytes = GzipDecoder::new(gz_bytes);
    let mut entries = AsyncReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .create_deserializer(tsv_bytes)
        .into_deserialize::<Entry>();
    let mut stats = FileStatsBuilder::new(args);
    while let Some(entry) = entries.next().await {
        stats.add_entry(entry.with_context(context)?);
    }
    Ok(stats.finish_file())
}

/// Entry from the dataset
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct Entry {
    /// (Case-sensitive) n-gram whose frequency is being studied
    //
    // TODO: If string allocation/deallocation becomes a bottleneck, study
    //       in-place deserialization like rkyv.
    pub ngram: Ngram,

    /// Year in which this frequency was recorded
    pub year: Year,

    /// Number of matches
    pub match_count: NonZeroUsize,

    /// Number of books in which matches were found
    pub volume_count: NonZeroUsize,
}

// TODO: Extract stats to their own module

/// Cumulative knowledge from a data file
#[derive(Debug)]
pub struct FileStatsBuilder {
    /// Data collection configuration
    config: Arc<Args>,

    /// Last n-gram seen within the file, if any
    current_ngram: Option<Ngram>,

    /// Accumulated stats from accepted entries for current_ngram, if any
    current_stats: Option<NgramStats>,

    /// Accumulated stats across n-gram case equivalence classes
    ///
    /// For each n-gram case equivalence class, provides...
    /// - Total stats across the case equivalence class
    /// - Current case with top stats, and stats for this case
    case_stats: HashMap<UniCase<Ngram>, CaseStats>,
}
//
impl FileStatsBuilder {
    /// Set up the accumulator
    pub fn new(config: Arc<Args>) -> Self {
        Self {
            config,
            current_ngram: None,
            current_stats: None,
            case_stats: HashMap::new(),
        }
    }

    /// Integrate a new dataset entry
    ///
    /// Dataset entries should be added in the order where they come in data
    /// files: sorted by ngram, then by year.
    pub fn add_entry(&mut self, entry: Entry) {
        // Make sure there is a current n-gram
        let current_ngram = self.current_ngram.get_or_insert_with(|| entry.ngram.clone());

        // Reject various flavors of invalid entries
        let too_old = entry.year < self.config.min_year();
        let not_a_word = entry.ngram.contains('_');
        if too_old || not_a_word {
            #[cfg(feature = "log-trace")]
            if *current_ngram != entry.ngram {
                let cause = if too_old {
                    "it's too old"
                } else if not_a_word {
                    "it isn't a word"
                } else {
                    unimplemented!()
                };
                log::trace!("Rejected {entry:?} because {cause} (will not report further rejections for this n-gram)");
                self.switch_ngram(Some(entry.ngram.clone()), None);
            }
            return;
        }

        // If the entry is associated with the current n-gram, merge it into the
        // current n-gram's statistics
        if let Some(current_stats) = &mut self.current_stats {
            if *current_ngram == entry.ngram {
                current_stats.add_entry(entry);
                return;
            }
        }

        // Otherwise, flush the current n-gram statistics and make the current
        // entry the new current n-gram
        self.switch_ngram(Some(entry.ngram.clone()), Some(entry.into()));
    }

    /// Export final statistics at end of dataset processing
    pub fn finish_file(mut self) -> FileStats {
        self.switch_ngram(None, None);
        self.case_stats
    }

    /// Integrate the current n-gram into the file statistics and switch to a
    /// different one (or none at all)
    ///
    /// This should be done when it is established that no other entry for this
    /// n-gram will come, either because we just moved to a different n-gram
    /// within the data file or because we reached the end of the data file.
    fn switch_ngram(&mut self, new_ngram: Option<Ngram>, new_stats: Option<NgramStats>) {
        // If there are stats, there must be an associated n-gram
        assert!(
            !(new_stats.is_some() && new_ngram.is_none()),
            "attempted to add stats without an associated n-gram"
        );

        // Update current n-gram and flush former n-gram stats, if any
        let former_ngram = std::mem::replace(&mut self.current_ngram, new_ngram);
        if let Some(former_stats) = std::mem::replace(&mut self.current_stats, new_stats) {
            // If there are stats, there must be an associated ngram
            let former_ngram = former_ngram
                .expect("current_stats should be associated with a current_ngram");

            // Check if there are sufficient statistics to accept this n-gram
            if former_stats.match_count.get() >= self.config.min_matches
                && former_stats.min_volume_count.get() >= self.config.min_books
            {
                // If so, inject it into the global file statistics
                log::trace!("Accepted n-gram {former_ngram:?} with {former_stats:?} into current file statistics");
                let entry = (former_ngram, former_stats);
                match self
                    .case_stats
                    .entry(UniCase::new(entry.0.clone()))
                {
                    hash_map::Entry::Occupied(o) => {
                        let o = o.into_mut();
                        log::trace!("Merged into existing case-equivalence class {o:#?}");
                        o.add_ngram(entry);
                        log::trace!("Result of equivalence class merging is {o:#?}");
                    }
                    hash_map::Entry::Vacant(v) => {
                        v.insert(CaseStats::from(entry));
                    }
                }
            } else {
                // If not, just log it for posterity
                log::trace!("Rejected n-gram {former_ngram:?} with {former_stats:?} from file statistics due to insufficient occurences");
            }
        }
    }
}

/// Cumulative knowledge from a data file
pub type FileStats = HashMap<UniCase<Ngram>, CaseStats>;

/// Cumulative knowledge about an n-gram
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct NgramStats {
    /// Year of first occurence
    pub first_year: Year,

    /// Year of last occurence
    pub last_year: Year,

    /// Total number of matches over period of interest
    pub match_count: NonZeroUsize,

    /// Lower bound on the number of books with matches over period of interest
    ///
    /// Is an exact count when the stats only cover a single n-gram casing, but
    /// becomes a lower bound as soon as case classes are merged.
    pub min_volume_count: NonZeroUsize,
}
//
impl NgramStats {
    /// Update statistics with a new yearly entry
    ///
    /// Yearly entries should be merged in the order that they appear in data
    /// files, i.e. from least recent to most recent.
    pub fn add_entry(&mut self, entry: Entry) {
        debug_assert!(
            self.first_year <= self.last_year,
            "Violated first < last year type invariant"
        );
        assert!(
            entry.year > self.first_year.max(self.last_year),
            "Dataset entries should be sorted by year"
        );
        self.last_year = entry.year;
        self.match_count = add_nonzero_usize(self.match_count, entry.match_count);
        // We can add volume counts here because we're still case-sensitive at
        // this point in time and books only have one publication date so there
        // is no aliasing between different years)
        self.min_volume_count = add_nonzero_usize(self.min_volume_count, entry.volume_count);
    }

    /// Merge statistics from two case-equivalent ngrams*
    ///
    /// Once you start doing this, you should stop calling add_entry.
    pub fn merge(&mut self, rhs: NgramStats) {
        self.first_year = self.first_year.min(rhs.first_year);
        self.last_year = self.last_year.max(rhs.last_year);
        self.match_count = add_nonzero_usize(self.match_count, rhs.match_count);
        // We cannot just add volume counts here because different casings of
        // the same word may appear within the same book
        self.min_volume_count = self.min_volume_count.max(rhs.min_volume_count);
    }
}
//
impl From<Entry> for NgramStats {
    fn from(entry: Entry) -> Self {
        Self {
            first_year: entry.year,
            last_year: entry.year,
            match_count: entry.match_count,
            min_volume_count: entry.volume_count,
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

/// Cumulative knowledge for a given n-gram case equivalence class
#[derive(Debug)]
pub struct CaseStats {
    /// Accumulated stats across the entire case equivalence class
    total_stats: NgramStats,

    /// Casing with the best stats
    top_ngram: Ngram,

    /// Case-sensitive stats for this casing
    top_stats: NgramStats,
}
//
impl CaseStats {
    /// Add a new case-equivalent n-gram to these stats
    ///
    /// It is assumed that you have checked that this n-gram is indeed
    /// case-equivalent to the one that the stats were created with.
    pub fn add_ngram(&mut self, (ngram, stats): (Ngram, NgramStats)) {
        // Sanity checks
        debug_assert!(
            self.total_stats >= self.top_stats,
            "total_stats should integrate the top casing's stats and then some"
        );
        assert_ne!(
            ngram, self.top_ngram,
            "this n-gram was already added to this case class!"
        );

        // Add n-gram statistics to the case-equivalent statistics
        self.total_stats.merge(stats);

        // If this n-gram has better stats than the previous top ngram, it
        // becomes the new top n-gram.
        if stats > self.top_stats {
            self.top_ngram = ngram;
            self.top_stats = stats;
        }
    }

    /// Merge case equivalent statistics from another data file into these ones
    ///
    /// You should have checked that the case equivalence class you're merging
    /// is indeed equivalent to this one.
    pub fn merge(&mut self, rhs: CaseStats) {
        debug_assert!(
            self.total_stats >= self.top_stats,
            "total_stats should integrate the top casing's stats and then some"
        );
        self.total_stats.merge(rhs.total_stats);
        if rhs.top_ngram == self.top_ngram {
            self.top_stats.merge(rhs.top_stats);
        } else if rhs.top_stats > self.top_stats {
            self.top_ngram = rhs.top_ngram;
            self.top_stats = rhs.top_stats;
        }
    }
}
//
impl From<(Ngram, NgramStats)> for CaseStats {
    fn from((ngram, stats): (Ngram, NgramStats)) -> Self {
        Self {
            total_stats: stats,
            top_ngram: ngram,
            top_stats: stats,
        }
    }
}

/// Year of Gregorian Calendar
pub type Year = isize;

/// Case-sensitive n-gram
pub type Ngram = Box<str>;

/// Addition operator for NonZeroUsize
pub fn add_nonzero_usize(x: NonZeroUsize, y: NonZeroUsize) -> NonZeroUsize {
    NonZeroUsize::new(x.get() + y.get()).expect("overflow while adding NonZeroUsizes")
}
