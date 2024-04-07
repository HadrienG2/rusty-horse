//! This program is based on the Google Books Ngrams dataset, whose general
//! documentation you can find at
//! <http://storage.googleapis.com/books/ngrams/books/datasetsv3.html>.

mod config;
mod file;
mod languages;
mod progress;
mod stats;
mod top;

use crate::{config::Config, progress::ProgressReport, stats::FileStats};
use anyhow::Context;
use clap::Parser;
use log::LevelFilter;
use std::{collections::hash_map, num::NonZeroUsize};
use tokio::task::JoinSet;

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
    #[arg(long, default_value_t = true)]
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
    #[arg(short = 'm', long, default_value_t = 6000)]
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
    #[arg(short = 'o', long)]
    max_outputs: Option<NonZeroUsize>,

    /// Sort output n-grams in order of decreasing match count
    ///
    /// When adjusting rejection settings, it is usually best to order outputs
    /// by decreasing occurence count. But that requires some post-processing,
    /// and is unnecessary in the usual workflow where words are randomly picked
    /// from the list. Therefore, consider disabling this once you're done
    /// tuning the filter cut-offs.
    #[arg(short, long, default_value_t = false)]
    sort_by_popularity: bool,
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

    // Pick a book language
    let language = languages::pick(&args)?;
    let dataset_urls = language.dataset_urls().collect::<Vec<_>>();

    // Set up progress reporting
    let report = ProgressReport::new(dataset_urls.len());

    // Start all the data file downloading and processing
    let config = Config::new(args, language);
    let client = reqwest::Client::new();
    let mut data_files = JoinSet::new();
    for url in dataset_urls {
        data_files.spawn(file::download_and_process(
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

    // Pick the most frequent n-grams across all data files
    let ngrams_by_decreasing_stats = top::pick_top_ngrams(&config, dataset_stats, &report);
    for ngram in ngrams_by_decreasing_stats {
        println!("{ngram}");
    }
    Ok(())
}

/// Use anyhow for Result type erasure
pub use anyhow::Result;

/// Year where the dataset that we use was published
pub const DATASET_PUBLICATION_YEAR: Year = 2012;

/// Case-sensitive n-gram
pub type Ngram = Box<str>;

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
