//! Processing of an gzipped TSV data files from Google

pub mod filter;

use crate::{
    config::Config,
    dataset::{
        builder::{DatasetBuilder, DatasetFiles},
        Dataset,
    },
    progress::{ProgressConfig, ProgressReport, ProgressTracker, Work},
    Ngram, Result, Year, YearData, YearMatchCount, YearVolumeCount,
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

/// Download a set of data files, extract their data and collect it in one place
pub async fn download_and_collect(
    config: Arc<Config>,
    client: reqwest::Client,
    urls: Vec<Box<str>>,
    report: &ProgressReport,
) -> Result<Arc<Dataset>> {
    // Track file downloads
    let num_files = urls.len();
    let downloads = report.add(
        "Initiating data downloads",
        ProgressConfig::new(Work::Steps(num_files)).dont_show_rate_eta(),
    );
    let bytes = report.add(
        "Downloading and extracting data",
        ProgressConfig::new(Work::Bytes(0)).allow_adding_work(),
    );

    // Start file downloads
    let mut data_files = JoinSet::new();
    for url in urls {
        data_files.spawn(download_and_extract(
            config.clone(),
            client.clone(),
            url,
            downloads.clone(),
            bytes.clone(),
        ));
    }

    // Collect and merge statistics from data files as downloads finish
    let mut dataset = DatasetFiles::new(config);
    while let Some(file_data) = data_files.join_next().await {
        dataset.merge(file_data.context("collecting results from one data file")??)
    }
    Ok(dataset.finish(report))
}

/// Download a data file and extract the data inside
pub async fn download_and_extract(
    config: Arc<Config>,
    client: reqwest::Client,
    url: Box<str>,
    downloads: ProgressTracker,
    bytes: ProgressTracker,
) -> Result<DatasetFiles> {
    // Start the download
    let context = || format!("initiating download of {url}");
    let response = client
        .get(&*url)
        .send()
        .await
        .and_then(Response::error_for_status)
        .with_context(context)?;
    bytes.add_work(response.content_length().with_context(context)?);
    if downloads.make_progress(1) {
        bytes.done_adding_work();
    }

    // Slice the download into chunks of bytes
    let gz_bytes = StreamReader::new(response.bytes_stream().map(move |res| {
        res
            // Track how many input bytes have been downloaded so far
            .inspect(|bytes_block| {
                bytes.make_progress(bytes_block.len() as u64);
            })
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
    let mut early_filter = filter::make_early_filter(config.clone());
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
