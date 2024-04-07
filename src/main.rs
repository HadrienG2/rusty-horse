//! This program is based on the Google Books Ngrams dataset, whose general
//! documentation you can find at
//! <http://storage.googleapis.com/books/ngrams/books/datasetsv3.html>.

mod languages;
mod progress;
mod stats;

use crate::{
    progress::ProgressReport,
    stats::{FileStats, FileStatsBuilder},
};
use anyhow::Context;
use async_compression::tokio::bufread::GzipDecoder;
use clap::Parser;
use csv_async::AsyncReaderBuilder;
use futures::stream::StreamExt;
use languages::LanguageInfo;
use log::LevelFilter;
use reqwest::Response;
use serde::Deserialize;
use std::{
    cmp::Reverse,
    collections::{hash_map, BinaryHeap, VecDeque},
    io::{self, ErrorKind},
    num::NonZeroUsize,
    sync::Arc,
};
use tokio::task::JoinSet;
use tokio_util::io::StreamReader;

/// TODO: User-visible program description
///
/// All occurence count cutoffs are applied to the total occurence count of a
/// single casing of the word, over the time period of interest.
#[derive(Parser, Debug)]
struct Args {
    /// Short name of the Google Ngrams language to be used, e.g. "eng-fiction"
    ///
    /// Will interactively prompt for a supported language if not specified.
    #[arg(short, long, default_value = None)]
    language: Option<Box<str>>,

    // TODO: "Bad word" exclusion mechanism
    //
    /// Strip capitalized words, if it makes sense for the target language
    ///
    /// In most languages from the Google Books dataset, n-grams that start with
    /// a capital letter tend to be an odd passphrase building block. But this
    /// is not always true, one exception being German. By default, we ignore
    /// capitalized words for every language where it makes sense to do so.
    #[arg(short, long, default_value_t = true)]
    strip_odd_capitalized: bool,

    /// Minimum accepted book publication year
    ///
    /// Our data set is based on books, many of which have been published a long
    /// time ago and may thus not represent modern language usage. You can
    /// compensate for this bias by ignoring books published before a certain
    /// year, at the cost of reducing the size of the dataset.
    ///
    /// By default, we include books starting 50 years before the date where the
    /// n-grams dataset was published.
    #[arg(short = 'y', long, default_value = None)]
    min_year: Option<Year>,

    /// Minimum accepted number of matches across of books
    ///
    /// Extremely rare words are not significantly more memorable than random
    /// characters. Therefore we ignore words which occur too rarely in the
    /// selected dataset section.
    #[arg(short = 'm', long, default_value_t = 5000)]
    min_matches: usize,

    /// Minimum accepted number of matching books
    ///
    /// If a word only appears in a book or two, it may be a neologism from the
    /// author, or the product of an error in the book -> text conversion
    /// process. Therefore, we only consider words which are seen in a
    /// sufficiently large number of books.
    #[arg(short = 'b', long, default_value_t = 10)]
    min_books: usize,

    /// Max number of output n-grams
    ///
    /// While it is possible to compute the full list of valid n-grams and trim
    /// it to the desired length later on, knowing the desired number of matches
    /// right from the start allows this program to discard less frequent
    /// n-grams before the full list of n-grams is available. As a result, the
    /// processing will consume less memory and run a little faster.
    #[arg(short = 'n', long, default_value = "32768")]
    max_outputs: Option<NonZeroUsize>,
}
//
impl Args {
    /// Decode and validate CLI arguments
    pub fn parse_and_check() -> Result<Self> {
        // Decode CLI arguments
        let args = Args::parse();

        // Check CLI arguments for basic sanity
        if let Some(min_year) = args.min_year {
            anyhow::ensure!(
                min_year <= DATASET_PUBLICATION_YEAR,
                "requested minimum publication year excludes all books from the dataset"
            );
        }
        Ok(args)
    }

    /// Minimal book publication year cutoff
    pub fn min_year(&self) -> Year {
        self.min_year.unwrap_or(DATASET_PUBLICATION_YEAR - 50)
    }
}
//
#[tokio::main]
async fn main() -> Result<()> {
    // Set up logging
    setup_logging().map_err(|e| anyhow::format_err!("{e}"))?;

    // Decode CLI arguments
    let args = Args::parse_and_check()?;

    // Let the user pick a language
    let language = if let Some(language) = &args.language {
        languages::get(language)?
    } else {
        languages::prompt()?
    };
    let dataset_urls = language.dataset_urls().collect::<Vec<_>>();
    let config = Config::new(args, language);

    // Set up progress reporting
    let report = ProgressReport::new(dataset_urls.len());

    // Start all the data file downloading and processing
    let client = reqwest::Client::new();
    let mut data_files = JoinSet::new();
    for url in dataset_urls {
        data_files.spawn(download_and_process(
            config.clone(),
            client.clone(),
            url,
            report.clone(),
        ));
    }

    // Collect and merge statistics from data files as downloads finish
    let mut dataset_stats = FileStats::new();
    while let Some(file_stats) = data_files.join_next().await {
        for (name, stats) in file_stats.context("collecting results from one data file")?? {
            match dataset_stats.entry(name) {
                hash_map::Entry::Occupied(o) => o.into_mut().merge_files(stats),
                hash_map::Entry::Vacant(v) => {
                    v.insert(stats);
                }
            }
        }
    }

    // Sort n-grams by frequency and pick the most frequent ones if requested
    report.start_sort(dataset_stats.len());
    let mut top_entries = if let Some(max_outputs) = config.max_outputs {
        BinaryHeap::with_capacity(max_outputs.get())
    } else {
        BinaryHeap::with_capacity(dataset_stats.len())
    };
    for (_, case_stats) in dataset_stats.drain() {
        let (top_ngram, total_stats) = case_stats.collect();
        top_entries.push((Reverse(total_stats), top_ngram));
        if let Some(max_outputs) = config.max_outputs {
            if top_entries.len() > max_outputs.get() {
                top_entries.pop();
            }
        }
        // NOTE: If these atomic increments become too expensive, use
        //       batched iteration to amortize them
        report.inc_sorted(1);
    }

    // Discard stats and reorder the results by decreasing frequency for final
    // display
    let mut ngrams_by_decreasing_stats = VecDeque::with_capacity(top_entries.len());
    while let Some((_, ngram)) = top_entries.pop() {
        ngrams_by_decreasing_stats.push_front(ngram);
    }
    for ngram in ngrams_by_decreasing_stats {
        println!("{ngram}");
    }

    Ok(())
}

/// Final process configuration
///
/// This is the result of combining digested [`Args`] with language-specific
/// considerations.
#[derive(Debug)]
pub struct Config {
    // TODO: "Bad word" exclusion mechanism
    //
    /// Whether capitalized n-grams should be ignored
    strip_capitalized: bool,

    /// Minimum accepted book publication year
    min_year: Year,

    /// Minimum accepted number of matches across of books
    min_matches: usize,

    /// Minimum accepted number of matching books
    min_books: usize,

    /// Maximal number of output n-grams
    max_outputs: Option<NonZeroUsize>,
}
//
impl Config {
    /// Determine process configuration from initialization products
    fn new(args: Args, language: LanguageInfo) -> Arc<Self> {
        let min_year = args.min_year();
        let Args { language: _, strip_odd_capitalized, min_year: _, min_matches, min_books, max_outputs } = args;
        Arc::new(Self {
            strip_capitalized: strip_odd_capitalized && language.should_strip_capitalized,
            min_year,
            min_matches,
            min_books,
            max_outputs,
        })
    }
}

/// Year where the dataset that we use was published
pub const DATASET_PUBLICATION_YEAR: Year = 2012;

/// Entry from the dataset
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct Entry {
    /// (Case-sensitive) n-gram whose frequency is being studied
    pub ngram: Ngram,

    /// Year in which this frequency was recorded
    pub year: Year,

    /// Number of matches
    pub match_count: NonZeroUsize,

    /// Number of books in which matches were found
    pub volume_count: NonZeroUsize,
}

/// Case-sensitive n-gram
pub type Ngram = Box<str>;

/// Use anyhow for Result type erasure
pub use anyhow::Result;

/// Year of Gregorian Calendar
pub type Year = isize;

/// Addition operator for NonZeroUsize
pub fn add_nonzero_usize(x: NonZeroUsize, y: NonZeroUsize) -> NonZeroUsize {
    NonZeroUsize::new(x.get() + y.get()).expect("overflow while adding NonZeroUsizes")
}

/// Set up logging
fn setup_logging() -> syslog::Result<()> {
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
}

/// Start downloading and processing a data file
async fn download_and_process(
    config: Arc<Config>,
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

    // Slice the download into chunks of bytes
    let gz_bytes = StreamReader::new(response.bytes_stream().map(|res| {
        res
            // Track how many input bytes have been downloaded so far
            .inspect(|bytes_block| report.inc_bytes(bytes_block.len()))
            // Translate reqwest errors into I/O errors
            .map_err(|e| io::Error::new(ErrorKind::Other, Box::new(e)))
    }));

    // Apply gzip decoder to compressed bytes
    let tsv_bytes = GzipDecoder::new(gz_bytes);

    // Apply TSV decoder to uncompressed bytes
    let mut entries = AsyncReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .create_deserializer(tsv_bytes)
        .into_deserialize::<Entry>();

    // Accumulate statistics from TSV entries
    let mut stats = FileStatsBuilder::new(config.clone());
    let context = || format!("fetching and processing {url}");
    while let Some(entry) = entries.next().await {
        stats.add_entry(entry.with_context(context)?);
    }
    Ok(stats.finish_file())
}
