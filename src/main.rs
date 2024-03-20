//! This program is based on the Google Books Ngrams dataset, whose general
//! documentation you can find at
//! <http://storage.googleapis.com/books/ngrams/books/datasetsv3.html>.

mod languages;
mod progress;

use std::sync::Arc;

use crate::progress::ProgressReport;
use anyhow::Context;
use tokio::task::JoinSet;
use futures::StreamExt;

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

    // Wait for all downloads to complete
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
    report: Arc<ProgressReport>
) -> Result<()> {
    // Start the download
    let context = || format!("Initiating download of data file {url}");
    let response = client.get(&*url).send().await.with_context(context)?;
    report.start_download(response.content_length().with_context(context)?);

    // Process the byte stream
    let context = || format!("Downloading data file {url}");
    let mut bytes_stream = response.bytes_stream();
    while let Some(bytes_block) = bytes_stream.next().await {
        let bytes_block = bytes_block.with_context(context)?;
        // FIXME: Actually do something useful with those bytes,
        //        produce results and eventually return them.
        report.inc_bytes(bytes_block.len());
    }
    Ok(())
}
