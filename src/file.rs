//! Processing of an individual data file

use crate::{
    config::Config, dataset::{builder::{DatasetBuilder, DatasetFiles}, Dataset}, progress::ProgressReport, Ngram, Result, Year, YearMatchCount, YearVolumeCount
};
use anyhow::Context;
use async_compression::tokio::bufread::GzipDecoder;
use csv_async::AsyncReaderBuilder;
use futures::{future, stream::StreamExt, TryStreamExt};
use reqwest::Response;
use serde::Deserialize;
use std::{
    io::{self, ErrorKind},
    sync::Arc,
};
use tokio::task::JoinSet;
use tokio_util::io::StreamReader;

/// Download a set of data files, extract their data and collect it in one place
pub async fn download_and_collect(
    config: Arc<Config>,
    client: reqwest::Client,
    urls: impl IntoIterator<Item = Box<str>>,
    report: Arc<ProgressReport>,
) -> Result<Dataset> {
    // Start downloading and processing all the files
    let mut data_files = JoinSet::new();
    for url in urls {
        data_files.spawn(download_and_extract(
            config.clone(),
            client.clone(),
            url,
            report.clone(),
        ));
    }

    // Collect and merge statistics from data files as downloads finish
    let mut dataset = DatasetFiles::new(config);
    while let Some(file_stats) = data_files.join_next().await {
        dataset.merge(
            file_stats.context("collecting results from one data file")??
        )
    }
    Ok(dataset.finish())
}

/// Download a data file and extract the data inside
pub async fn download_and_extract(
    config: Arc<Config>,
    client: reqwest::Client,
    url: Box<str>,
    report: Arc<ProgressReport>,
) -> Result<DatasetFiles> {
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

    // Accumulate data from TSV entries
    let mut dataset = DatasetBuilder::new(config);
    let context = || format!("fetching and processing {url}");
    while let Some(entry) = entries.next().await {
        let entry = entry.with_context(context)?;
        dataset.add_entry(entry.clone());
    }
    Ok(dataset.finish_file())
}

/// Entry from the dataset
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
pub struct Entry {
    /// (Case-sensitive) ngram whose frequency is being studied
    pub ngram: Ngram,

    // FIXME: In my ideal world, this would be #[serde(flatten)] YearData, but
    //        the csv + serde combination rejects this for unknown reasons.
    //
    /// Year on which the data was recorded
    year: Year,

    /// Number of recorded occurences
    match_count: YearMatchCount,

    /// Number of books across which occurences were recorded
    volume_count: YearVolumeCount,
}
//
impl Entry {
    /// Yearly data subset of this entry
    pub fn data(&self) -> YearData {
        YearData {
            year: self.year,
            match_count: self.match_count,
            volume_count: self.volume_count,
        }
    }
}

/// Yearly data subset of a dataset entry
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq)]
pub struct YearData {
    /// Year on which the data was recorded
    pub year: Year,

    /// Number of recorded occurences
    pub match_count: YearMatchCount,

    /// Number of books across which occurences were recorded
    pub volume_count: YearVolumeCount,
    
}

/// Build the early entry filter
///
/// Data file entries go through this filter first before undergoing any other
/// processing. This avoids unnecessary processing in scenarios where just by
/// looking at an entry we can quickly infer that it should be thrown away.
///
/// One should refrain from performing expensive operations on the ngram field
/// of the entry at this filtering stage, because this field will appear
/// repeatedly in the file (once per year), and the first step of the processing
/// pipeline is to deduplicate those occurences. A second run of filtering will
/// be performed on the deduplicated ngram after this process has occured: the
/// normalizing filter (see below).
pub fn make_early_filter(config: Arc<Config>) -> impl FnMut(&Entry) -> bool {
    move |entry| {
        /// Reasons why a data file entry could be discarded
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        enum RejectCause {
            /// Entry is too old, may not reflect modern language usage
            Old,

            /// Entry starts with a capital letter and we're ignoring these
            Capitalized,
        }

        // Determine if an entry should be rejected
        let rejection = if entry.data().year < config.min_year {
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
        } else {
            None
        };

        // Report it in trace logs, but not repeatedly across consecutive years
        if let Some(rejection) = rejection {
            let cause = match rejection {
                RejectCause::Old => "it's too old",
                RejectCause::Capitalized => "capitalized ngrams are rejected",
            };
            log::trace!("Rejected {entry:?} because {cause}");
        }

        // Propagate entry filtering decision to the caller
        rejection.is_none()
    }
}

/// Normalize an ngram, or reject it as something that's not a dictionary word
///
/// This transformation and filtering process is to be performed after all
/// entries associated with an ngram have been processed, and preferably after
/// statistical cuts applying across the sum for all entries for an ngram have
/// been applied, but before the transformed ngram is recorded in a word list.
///
/// Some of the Google Books dataset ngrams (but not all of them!) contain tags
/// like _VERB that hint at their grammatical function. This is not something
/// that we care about for password generation purposes, so we discard this
/// information, effectively merging the data from entries with or witout it.
///
/// Some supported languages also feature ngrams that are composed of multiple
/// independent words linked via some non-whitespace punctuation. For example,
/// in French, "j'ai" is a contraction of "je" and "ai". Retrieving all of the
/// original words can be difficult (as you can see above, some letters tend to
/// be dropped in the process), so for now we discard these ngrams.
pub fn normalizing_filter_map(ngram: Ngram) -> Option<Ngram> {
    /// Reasons why an ngram could be discarded
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum RejectCause {
        /// Ngram makes unexpected use of underscores
        UnexpectedUnderscore,

        /// Ngram is not a dictionary word
        NonWord,
    }
    let log_rejection = |ngram: &str, cause| {
        let cause = match cause {
            RejectCause::UnexpectedUnderscore => "it uses underscores unexpectedly",
            RejectCause::NonWord => "it does not look like a dictionary word",
        };
        log::trace!("Rejected ngram {ngram:?} because {cause}");
    };

    // Remove grammar tags from the ngram
    let ngram = match remove_grammar_tags(ngram) {
        Ok(wo_tags) => wo_tags,
        Err(non_word) => {
            log_rejection(&non_word, RejectCause::UnexpectedUnderscore);
            log::trace!("Rejected ngram {non_word:?} because it uses underscores in an unexpected way");
            return None;
        }
    };

    // Reject ngrams which do not look like a dictionary word
    if !ngram.chars().all(char::is_alphabetic) {
        log_rejection(&ngram, RejectCause::NonWord);
        return None;
    }
    Some(ngram)
}



/// Remove well-formed grammar tags from an ngram, reject any other
/// underscore-based pattern which suggests we're dealing with a non-word entity
fn remove_grammar_tags(mut ngram: Ngram) -> Result<Ngram, Ngram> {
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
