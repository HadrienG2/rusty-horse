//! Processing of an individual data file

use crate::{
    config::Config, progress::ProgressReport, stats::{FileStats, FileStatsBuilder}, Ngram, Result, Year
};
use anyhow::Context;
use async_compression::tokio::bufread::GzipDecoder;
use csv_async::AsyncReaderBuilder;
use futures::{future, stream::StreamExt, TryStreamExt};
use reqwest::Response;
use serde::Deserialize;
use std::collections::hash_map;
use std::{
    io::{self, ErrorKind},
    num::NonZeroUsize,
    sync::Arc,
};
use tokio::task::JoinSet;
use tokio_util::io::StreamReader;

/// Download and process a set of data files, merge results
pub async fn download_and_process_all(
    config: Arc<Config>,
    client: reqwest::Client,
    urls: impl IntoIterator<Item = Box<str>>,
    report: Arc<ProgressReport>,
) -> Result<FileStats> {
    // Start downloading and processing all the files
    let mut data_files = JoinSet::new();
    for url in urls {
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
    Ok(dataset_stats)
}

/// Start downloading and processing a data file
pub async fn download_and_process(
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
    let gz_bytes = StreamReader::new(response.bytes_stream().map(move |res| {
        res
            // Track how many input bytes have been downloaded so far
            .inspect(|bytes_block| report.inc_bytes(bytes_block.len()))
            // Translate reqwest errors into I/O errors
            .map_err(|e| io::Error::new(ErrorKind::Other, Box::new(e)))
    }));

    // Apply gzip decoder to compressed bytes
    let tsv_bytes = GzipDecoder::new(gz_bytes);

    // Apply TSV decoder to uncompressed bytes
    let entries = AsyncReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .create_deserializer(tsv_bytes)
        .into_deserialize::<Entry>();

    // Filter out TSV entries that we know to be inappropriate early on
    let mut early_filter = make_early_filter(config.clone());
    let mut entries = entries.try_filter(move |entry| future::ready(early_filter(entry)));

    // Accumulate statistics from TSV entries
    let mut stats = FileStatsBuilder::new(config.clone());
    let context = || format!("fetching and processing {url}");
    while let Some(entry) = entries.next().await {
        stats.add_entry(entry.with_context(context)?);
    }
    Ok(stats.finish_file())
}

/// Entry from the dataset
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct Entry {
    /// (Case-sensitive) ngram whose frequency is being studied
    pub ngram: Ngram,

    /// Year in which this frequency was recorded
    pub year: Year,

    /// Number of matches
    pub match_count: NonZeroUsize,

    /// Number of books in which matches were found
    pub volume_count: NonZeroUsize,
}

/// Remove well-formed grammar tags from an ngram, reject any other
/// underscore-based pattern which suggests we're dealing with a non-word entity
pub fn remove_grammar_tags(mut ngram: Ngram) -> Result<Ngram, Ngram> {
    let mut remainder = &ngram[..];
    let mut new_ngram = String::new();
    // A grammar tag starts with an underscore sign...
    'strip_tags: while let Some((before, after)) = remainder.split_once('_') {
        new_ngram.push_str(before);
        'find_tag_end: for (idx, c) in after.char_indices() {
            if c.is_ascii_uppercase() {
                // ...followed by 1+ uppercase ASCII letters...
                continue 'find_tag_end;
            } else if idx >= 1 && c == '_' {
                // ...and an optional terminating underscore
                remainder = if idx + 1 < after.len() {
                    &after[idx+1..]
                } else {
                    ""
                };
                continue 'strip_tags;
            } else {
                // Anything else indicates that this is not a grammar tag, but
                // another weird use of underscores that has no place in a word
                return Err(ngram);
            }
        }
        // Absence of terminating underscore is okay, it just means we stop here
        break 'strip_tags;
    }
    if !new_ngram.is_empty() {
        log::trace!("Normalized tagged ngram {ngram:?} into untagged form {new_ngram:?}");
        ngram = new_ngram.into();
    }
    Ok(ngram)
}

/// Build the early entry filter
///
/// Data file entries go through this filter first before undergoing any other
/// processing. This avoids unnecessary processing in scenarios where just by
/// looking at an entry we can quickly infer that it should be thrown away.
///
/// One should refrain from performing expensive on an ngram at this stage
/// because
pub fn make_early_filter(config: Arc<Config>) -> impl FnMut(&Entry) -> bool {
    move |entry| {
        /// Reasons why a data file entry could be discarded
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        enum RejectCause {
            /// Entry is too old, may not reflect modern language usage
            Old,

            /// Entry starts with a capital letter and we're ignoring these
            Capitalized,

            /// Entry is not a dictionary word, not even a tagged one
            NonWord,
        }

        // Determine if an entry should be rejected
        let rejection = if entry.year < config.min_year {
            Some(RejectCause::Old)
        } else if config.strip_capitalized
            && entry
                .ngram
                .chars()
                .next()
                .expect("ngrams shouldn't be empty")
                .is_uppercase()
        {
            Some(RejectCause::Capitalized)
        } else if !entry.ngram.chars().all(|c| c.is_alphabetic() || c == '_') {
            Some(RejectCause::NonWord)
        } else {
            None
        };

        // Report it in trace logs, but not repeatedly across consecutive years
        if let Some(rejection) = rejection {
            let cause = match rejection {
                RejectCause::Old => "it's too old",
                RejectCause::Capitalized => "capitalized ngrams are rejected",
                RejectCause::NonWord => "it's not a word",
            };
            log::trace!("Rejected {entry:?} because {cause}");
        }

        // Propagate entry filtering decision to the caller
        rejection.is_none()
    }
}
