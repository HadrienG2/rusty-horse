//! Processing of an individual data file

use crate::{
    config::Config,
    progress::ProgressReport,
    stats::{FileStats, FileStatsBuilder},
    Ngram, Result, Year,
};
use anyhow::Context;
use async_compression::tokio::bufread::GzipDecoder;
use csv_async::AsyncReaderBuilder;
use futures::stream::StreamExt;
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
