//! This program is based on the Google Books Ngrams dataset, whose general
//! documentation you can find at
//! <http://storage.googleapis.com/books/ngrams/books/datasetsv3.html>.

mod languages;
mod progress;

use crate::progress::ProgressReport;
use anyhow::Context;
use async_compression::tokio::bufread::GzipDecoder;
use csv_async::AsyncReaderBuilder;
use futures::stream::StreamExt;
use reqwest::Response;
use serde::Deserialize;
use std::{
    io::{self, ErrorKind},
    sync::Arc,
};
use tokio::task::JoinSet;
use tokio_util::io::StreamReader;

/// Use anyhow for Result type erasure
pub use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Let the user pick a language
    let language = languages::ask_language()?;
    let dataset_urls = language.dataset_urls().collect::<Vec<_>>();

    // TODO: Ask user for an optional year cutoff. Data from books that were
    //       published on an earlier year will be ignored. Validate that
    //       specified year is earlier than dataset publication year.
    // TODO: Implement caching so we don't re-download everything every time

    // Set up progress reporting
    let report = ProgressReport::new(dataset_urls.len());

    // Start all the data file downloading and processing
    let client = reqwest::Client::new();
    let mut data_files = JoinSet::new();
    for url in dataset_urls {
        data_files.spawn(download_and_process(client.clone(), url, report.clone()));
    }

    // Wait for all data files to be fully processed
    while let Some(join_result) = data_files.join_next().await {
        // FIXME: Collect and merge results once they are produced
        join_result??;
    }
    Ok(())
}

/// Start downloading and processing a data file
async fn download_and_process(
    client: reqwest::Client,
    url: Box<str>,
    report: Arc<ProgressReport>,
) -> Result<()> {
    // Start the download
    let context = || format!("Initiating download of {url}");
    let response = client
        .get(&*url)
        .send()
        .await
        .and_then(Response::error_for_status)
        .with_context(context)?;
    report.start_download(response.content_length().with_context(context)?);

    // Process the byte stream
    let context = || format!("Fetching and decoding {url}");
    let gz_bytes = StreamReader::new(response.bytes_stream().map(|res| {
        res
            // Track how many input bytes have been downloaded so far
            .inspect(|bytes_block| report.inc_bytes(bytes_block.len()))
            // Translate reqwest errors into I/O errors
            .map_err(|e| io::Error::new(ErrorKind::Other, Box::new(e)))
    }));
    let tsv_bytes = GzipDecoder::new(gz_bytes);
    let tsv_deserializer = AsyncReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .create_deserializer(tsv_bytes);
    let mut entries = tsv_deserializer.into_deserialize::<Entry>();
    while let Some(entry) = entries.next().await {
        let entry = entry.with_context(context)?;
        // TODO: Do something useful with that record
        // report.multi.println(format!("{entry:?}"));
    }
    Ok(())
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
    pub year: isize,

    /// Number of matches
    pub match_count: usize,

    /// Number of books in which matches were found
    pub volume_count: usize,
}
