//! This program is based on the Google Books Ngram dataset, whose general
//! documentation you can find at
//! <http://storage.googleapis.com/books/ngrams/books/datasetsv3.html>.

mod config;
mod dataset;
mod languages;
mod progress;
mod stats;
mod top;
mod tsv;

use crate::{config::Config, progress::ProgressReport};
use anyhow::Context;
use clap::Parser;
use log::LevelFilter;
use serde::Deserialize;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use tokio::io::{AsyncWriteExt, BufWriter};

/// TODO: User-visible program description
///
/// All occurence count cutoffs are applied to the occurence count of a single
/// ngram (before case equivalence), over the time period of interest.
#[derive(Parser, Debug)]
#[command(version, author)]
struct Args {
    /// Short name of the Google Books Ngram language to be used, e.g.
    /// "eng-fiction"
    ///
    /// Will interactively prompt for a supported language if not specified.
    #[arg(short, long, default_value = None)]
    language: Option<Box<str>>,

    // TODO: "Bad word" exclusion mechanism
    //
    /// Ignore capitalized words from the source dataset
    ///
    /// In most languages from the Google Books dataset, ngrams that start with
    /// a capital letter tend to be an odd passphrase building block (proper
    /// noun, acronym, and other less-memorable stuff). However this is not
    /// always true, the most obvious exception being German where all common
    /// nouns start with a capital letter.
    ///
    /// By default, we ignore capitalized words for every language where it
    /// makes sense to do so.
    #[arg(long)]
    strip_capitalized: Option<bool>,

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

    /// Minimum accepted number of matches across all books
    ///
    /// Extremely rare words are not significantly more memorable than random
    /// characters. Therefore we ignore words which occur too rarely in the
    /// selected dataset section.
    #[arg(short = 'm', long, default_value = "6000")]
    min_matches: NonZeroU64,

    /// Minimum accepted number of matching books
    ///
    /// If a word only appears in a book or two, it may be a neologism from the
    /// author, or the product of an OCR error. Therefore, we only consider
    /// words which are seen in a sufficiently large number of books.
    #[arg(short = 'b', long, default_value = "10")]
    min_books: NonZeroU64,

    /// In-memory dataset chunk size
    ///
    /// Data which is resident in memory is sliced into chunks of a certain
    /// number of ngrams case equivalence classes, which are independent from
    /// each other. This enables easy parallelization and asynchronism.
    ///
    /// The associated chunk size is a tunable parameter, which should be
    /// adjusted until optimal performance is observed. If it is set too low,
    /// constant overheads for spawning parallel/asynchronous tasks will not be
    /// properly amortized. But if it is set too high, parallel load balancing
    /// will be less effective and CPU caches will be used less efficiently.
    #[arg(long, default_value = "500")]
    memory_chunk: NonZeroUsize,

    /// On-disk dataset chunk size
    ///
    /// This is the granularity with which dataset entries are written to or
    /// loaded from storage. It will be rounded up to the next multiple of the
    /// in-memory chunk size.
    //
    // FIXME: Tune this to sensible defaults
    #[arg(long, default_value = "500")]
    storage_chunk: NonZeroUsize,

    /// Max number of output ngrams
    ///
    /// While it is possible to compute the full list of valid ngrams and trim
    /// it to the desired length later on, knowing the desired number of matches
    /// right from the start allows this program to discard less frequent
    /// ngrams before the full list of ngrams is available. As a result, the
    /// processing will consume less memory and run a little faster.
    #[arg(short = 'o', long)]
    max_outputs: Option<NonZeroUsize>,

    /// Sort output ngrams in order of decreasing match count
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

    /// Storage chunk size
    pub fn mem_chunks_per_storage_chunk(&self) -> NonZeroUsize {
        NonZeroUsize::new(
            self.storage_chunk.get().div_ceil(self.memory_chunk.get()),
        )
        .expect("rounding the disk chunk size to the next multiple of the memory chunk size shouldn't break the NonZero property")
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
    let report = ProgressReport::new();

    // Collect the dataset (TODO: Try to use the cache here)
    let config = Config::new(args, language);
    let client = reqwest::Client::new();
    let dataset = tsv::download_and_collect(config.clone(), client, dataset_urls, &report).await?;

    // Start caching the dataset on disk (TODO: Don't do this if it comes from
    // the cache already)
    let save_cache = tokio::spawn(dataset::cache::save(
        config.clone(),
        dataset.clone(),
        report.clone(),
    ));

    // Pick the most frequent ngrams across all data files
    let ngrams_by_decreasing_stats = top::pick_top_ngrams(&config, &dataset, &report);

    // Wait until we're done saving the dataset to disk
    save_cache.await.context("saving dataset to disk")??;

    // Display the most frequent ngrams
    {
        let stdout = tokio::io::stdout();
        let mut stdout = BufWriter::new(stdout);
        for ngram in ngrams_by_decreasing_stats {
            stdout.write_all(ngram.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
        }
        stdout.flush().await?;
    }
    Ok(())
}

/// Use anyhow for Result type erasure
pub use anyhow::Result;

/// Year where the dataset that we use was published
pub const DATASET_PUBLICATION_YEAR: Year = 2012;

/// Case-sensitive ngram
pub type Ngram = Box<str>;

/// Yearly data about an ngram's usage
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq)]
pub struct YearData {
    /// Year on which the data was recorded
    pub year: Year,

    /// Number of recorded occurences
    pub match_count: YearMatchCount,

    /// Number of books across which occurences were recorded
    pub volume_count: YearVolumeCount,
}

/// Year of Gregorian Calendar
pub type Year = i16;

/// Number of matches for an ngram over a single year
///
/// According to
/// https://github.com/orgtre/google-books-ngram-frequency?tab=readme-ov-file#the-underlying-corpus,
/// English can have >283 billion matches over 10 years, or an average of
/// 28B/year. Knowing that some words (like "the" in English) are a lot more
/// common than others, using u32s for yearly match counts would feel dangerous.
pub type YearMatchCount = NonZeroU64;

/// Number of books with matches for an ngram over a single year
///
/// According to
/// https://worldpopulationreview.com/country-rankings/books-published-per-year-by-country
/// there were ~4 million books published over a recent year, and according to
/// the total_counts file from the dataset Google only mined 220k of those
/// books. So u32 counts seem pretty fitting
pub type YearVolumeCount = NonZeroU32;

/// Addition operator for NonZeroU64
pub fn add_nz_u64(x: NonZeroU64, y: NonZeroU64) -> NonZeroU64 {
    NonZeroU64::new(x.get() + y.get()).expect("overflow while adding NonZeroU64s")
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

/// Use jemalloc for improved multi-thread performance
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
