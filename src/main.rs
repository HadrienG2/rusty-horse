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
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use reqwest::Response;
use serde::Deserialize;
use std::{
    cmp::Reverse, collections::BTreeMap, io::{self, ErrorKind}, sync::Arc
};
use tokio::task::JoinSet;
use tokio_util::io::StreamReader;

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
    language: Option<String>,

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
    // TODO: Fine-tune based on empirically observed irrelevance threshold
    #[arg(short = 'm', long, default_value_t = 100)]
    min_matches: usize,

    /// Minimum accepted number of matching books
    ///
    /// If a word only appears in a book or two, it may be a neologism from the
    /// author, or the product of an error in the book -> text conversion
    /// process. Therefore, we only consider words which appear across a
    /// sufficiently large number of books.
    //
    // TODO: Fine-tune based on empirically observed irrelevance threshold
    #[arg(short = 'b', long, default_value_t = 10)]
    min_books: usize,

    /// Max number of output n-grams
    ///
    /// While it is possible to compute the full list of valid n-grams and trim
    /// it to the desired length later on, knowing the desired number of matches
    /// right from the start allows this program to discard less frequent
    /// n-grams before the full list of n-grams is available. As a result, the
    /// processing will consume less memory and run a little faster.
    #[arg(short, long, default_value = None)]
    max_outputs: Option<usize>,
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
            "Requested minimum publication year excludes all books from the dataset"
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

    // Wait for all data files to be fully processed
    let mut full_stats = Vec::new();
    while let Some(join_result) = data_files.join_next().await {
        full_stats.push(join_result??);
    }

    // Sort n-grams by decreasing occurence frequency
    report.start_sort(full_stats.iter().map(|slice| slice.len()).sum());
    let full_stats = full_stats
        .into_par_iter()
        .map(Vec::from)
        .flatten()
        .inspect(|_| {
            // NOTE: If these atomic increments become too expensive, use
            //       batched iteration to amortize them
            report.inc_sorted(1)
        })
        .map(|stats| (Reverse(stats.match_count), stats))
        .collect::<BTreeMap<_, _>>();

    // TODO: Do something sensible with output
    println!("{full_stats:#?}");

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
    let context = || format!("Initiating download of {url}");
    let response = client
        .get(&*url)
        .send()
        .await
        .and_then(Response::error_for_status)
        .with_context(context)?;
    report.start_download(response.content_length().with_context(context)?);

    // Process the byte stream into deserialized TSV records
    let context = || format!("Fetching and processing {url}");
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
        stats.add(entry.with_context(context)?);
    }
    Ok(stats.finish())
}

/// Entry from the dataset
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct Entry {
    /// (Case-sensitive) n-gram whose frequency is being studied
    //
    // TODO: If string allocation/deallocation becomes a bottleneck, study
    //       frameworks for in-place deserialization like rkyv.
    pub ngram: String,

    /// Year in which this frequency was recorded
    pub year: Year,

    /// Number of matches
    pub match_count: usize,

    /// Number of books in which matches were found
    pub volume_count: usize,
}

/// Cumulative knowledge from a data file
#[derive(Clone, Debug)]
pub struct FileStatsBuilder {
    /// Data collection configuration
    config: Arc<Args>,

    /// Accumulated data
    // TODO: Introduce a case-insensitive case-preserving accumulation mechanism
    //       which tracks aggregated occurences across all case variants for
    //       popularity tracking purposes, but keeps the most frequent case
    //       variant at the end.
    ngrams: Vec<NgramStats>,

    /// Last n-gram that was ignored without being merged in stats
    #[cfg(feature = "log-trace")]
    last_ignored: String,
}
//
impl FileStatsBuilder {
    /// Set up the accumulator
    pub fn new(config: Arc<Args>) -> Self {
        Self {
            config,
            ngrams: Vec::new(),
            #[cfg(feature = "log-trace")]
            last_ignored: String::new(),
        }
    }

    /// Merge information from a data file entry
    pub fn add(&mut self, entry: Entry) {
        // Ignore entries that would never be valid
        let too_old = entry.year < self.config.min_year();
        let not_a_word = entry.ngram.contains('_');
        if too_old || not_a_word {
            #[cfg(feature = "log-trace")]
            if self.last_ignored != entry.ngram {
                if too_old {
                    log::trace!("Rejected {entry:?} because it is too old");
                } else if not_a_word {
                    log::trace!("Rejected {entry:?} because it is not a word");
                }
                self.last_ignored = entry.ngram.clone();
            }
            return;
        }

        // Update existing ngrams stat if they exist
        if let Some(stats) = self.ngrams.last_mut() {
            debug_assert!(stats.first_year <= stats.last_year);
            // If this is the same ngram, merge into its existing stats
            if *stats.ngram == entry.ngram {
                stats.add(entry);
                return;
            }

            // Once we move to the next n-gram, apply the notoriety cutoff to
            // the previous n-gram as its stats are now complete
            // TODO: Use a BTreeMap as a kind of bounded ring buffer to keep a
            //       rolling accumulator of the max_outputs most popular entries
            //       at any time. Do a debug log entries which pass the
            //       max_outputs cutoff for now.
            if stats.match_count < self.config.min_matches
                || stats.volume_count < self.config.min_books
            {
                log::trace!("Rejected {stats:?} because it is too obscure");
                self.ngrams.pop();
            } else {
                log::trace!("Done processing {stats:?}");
            }
        }

        // Add new stats for this ngram
        self.ngrams.push(NgramStats::from(entry));
    }

    // TODO: Add accumulator merging that checks the config is the same and
    //       keeps the top N entries at the end.

    /// Export statistics once done processing the file
    //
    // TODO: Ultimately, we only want to do this once done processing all files
    pub fn finish(self) -> FileStats {
        self.ngrams.into()
    }
}

/// Cumulative knowledge from a data file
pub type FileStats = Box<[NgramStats]>;

/// Cumulative knowledge about an n-gram
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NgramStats {
    /// Text of the n-gram
    pub ngram: Box<str>,

    /// Year of first occurence
    pub first_year: Year,

    /// Year of last occurence
    pub last_year: Year,

    /// Total number of matches over period of interest
    pub match_count: usize,

    /// Total number of books with matches over period of interest
    pub volume_count: usize,
}
//
impl NgramStats {
    /// Merge the data from one dataset entry
    pub fn add(&mut self, entry: Entry) {
        assert_eq!(
            *self.ngram, entry.ngram,
            "Attempted to integrate an incompatible entry"
        );
        assert!(
            entry.year > self.first_year.max(self.last_year),
            "Dataset entries should be sorted by year"
        );
        self.last_year = entry.year;
        self.match_count += entry.match_count;
        self.volume_count += entry.volume_count;
    }
}
//
impl From<Entry> for NgramStats {
    fn from(entry: Entry) -> Self {
        Self {
            ngram: entry.ngram.into(),
            first_year: entry.year,
            last_year: entry.year,
            match_count: entry.match_count,
            volume_count: entry.volume_count,
        }
    }
}

/// Year of Gregorian Calendar
pub type Year = isize;
