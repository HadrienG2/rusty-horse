//! Disk cache of the dataset

use crate::{config::Config, Result};
use anyhow::Context;
use arrow::{array::{ArrayRef, Int16Builder, LargeStringBuilder, MapBuilder, MapFieldNames, RecordBatch, UInt32Builder, UInt64Builder}, datatypes::{DataType, Field, Schema}};
use directories::ProjectDirs;
use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};
use std::{path::Path, sync::Arc};
use tokio::fs::{self, File};
use super::Dataset;

/// Save the current configuration and dataset into the cache
pub async fn save(config: Arc<Config>, dataset: Arc<Dataset>) -> Result<()> {
    // Look up where the cache should be saved
    //
    // FIXME: Don't write there directly, instead write to a temp directory,
    //        then replace existing cache with mv
    // FIXME: Should use one distinct cache per language => lang subdirectory
    let cache_dir = cache_dir().context("looking up the cache save location")?;

    // Save the app configuration that the cache was created with
    let config_path = cache_dir.join("config.json");
    let config_json = serde_json::to_vec_pretty(&*config).context("converting app configuration to JSON")?;
    fs::write(config_path, &config_json).await.context("saving JSON app configuration to disk")?;

    // In an ideal world, the dataset would be saved as one file in some sane
    // columnar data format.
    //
    // In the world we're living in, however, Apache Arrow is the only somewhat
    // mature columnar data framework with a readily available Rust interface,
    // and its support for structured data is unfortunately oriented towards
    // weakly typed languages like Python, with ergonomics that make it all but
    // unusable from a strongly typed language like Rust, except for the easy
    // special case of maps. Therefore, we avoid this feature and implement our
    // own structured data indexing over two Parquet files:
    //
    // - "year_data.parquet" is the concatenated yearly data from all ngrams
    // - "case_classes.parquet" contains ngrams, grouped by case equivalence
    //   classes, with pointers to the associated yearly data rows in year_data.
    // FIXME: Make compression level configurable from CLI
    let writer_properties = WriterProperties::builder()/*.set_compression(
        parquet::basic::Compression::ZSTD(
            ZstdLevel::try_new(1).context("picking zstd compression level")?
        )
    )*/.build();
    let year_data_schema = Arc::new(Schema::new(vec![
        // FIXME: Figure out how to use non-nullable data here
        Field::new("years", DataType::Int16, true),
        Field::new("match_counts", DataType::UInt64, true),
        Field::new("volume_counts", DataType::UInt32, true),
    ]));
    let mut year_data_writer = AsyncArrowWriter::try_new(
        File::create(cache_dir.join("year_data.parquet")).await.context("creating the yearly data file")?,
        year_data_schema.clone(),
        Some(writer_properties.clone()),
    ).context("preparing to write down case classes")?;
    let case_classes_schema = Arc::new(Schema::new(vec![
        Field::new_map(
            "case_classes",
            "ngram_to_data_end", 
            Field::new("ngrams", DataType::LargeUtf8, false),
            // FIXME: Figure out how to use non-nullable data here
            Field::new("data_ends", DataType::UInt64, true),
            false,
            false,
        )
    ]));
    let mut case_classes_writer = AsyncArrowWriter::try_new(
        File::create(cache_dir.join("case_classes.parquet")).await.context("creating the case classes file")?,
        case_classes_schema.clone(),
        Some(writer_properties.clone()),
    ).context("preparing to write down case classes")?;

    // Save the dataset using the user-requested chunk size
    let mut year_data_len = 0;
    for storage_chunk in dataset.blocks().chunks(config.mem_chunks_per_storage_chunk.get()) {
        // Collect data from the current storage chunk
        let mut case_classes = MapBuilder::new(
            Some(MapFieldNames {
                entry: "ngram_to_data_end".into(),
                key: "ngrams".into(),
                value: "data_ends".into(),
            }),
            LargeStringBuilder::new(),
            UInt64Builder::new(),
        );
        let mut years = Int16Builder::new();
        let mut match_counts = UInt64Builder::new();
        let mut volume_counts = UInt32Builder::new();
        for mem_chunk in storage_chunk {
            for case_class in mem_chunk.case_classes() {
                let (ngrams, data_ends) = case_classes.entries();
                for ngram_info in case_class.ngrams() {
                    for year_data in ngram_info.years() {
                        years.append_value(year_data.year);
                        match_counts.append_value(year_data.match_count.get());
                        volume_counts.append_value(year_data.volume_count.get());
                        year_data_len += 1;
                    }
                    ngrams.append_value(ngram_info.ngram());
                    data_ends.append_value(year_data_len);
                }
                case_classes.append(true).context("recording a case equivalence class")?;
            }
        }

        // Convert the collected data into RecordBatches and start writing them
        let years = years.finish();
        let match_counts = match_counts.finish();
        let volume_counts = volume_counts.finish();
        let year_data_batch = RecordBatch::try_new(
            year_data_schema.clone(),
            vec![
                Arc::new(years) as ArrayRef,
                Arc::new(match_counts) as ArrayRef,
                Arc::new(volume_counts) as ArrayRef
            ]
        ).context("creating a yearly data batch")?;
        let year_data_writer = year_data_writer.write(&year_data_batch);
        //
        let case_classes = case_classes.finish();
        let case_classes_batch = RecordBatch::try_new(
            case_classes_schema.clone(),
            vec![Arc::new(case_classes) as ArrayRef]
        ).context("creating a case classes data batch")?;
        let case_classes_writer = case_classes_writer.write(&case_classes_batch);

        // Wait for both I/O operations and handle I/O errors
        futures::try_join!(year_data_writer, case_classes_writer).context("submitting cache writes")?;
    }

    // Finish writing the cache files (FIXME: Implement atomicity here)
    futures::try_join!(
        year_data_writer.close(),
        case_classes_writer.close()
    ).context("closing cache files")?;
    Ok(())
}


/// Create the cache directory if it doesn't exist, and return its location
fn cache_dir() -> Result<Box<Path>> {
    let dirs = ProjectDirs::from("", "", env!("CARGO_PKG_NAME"))
        .context("determining the cache's location")?;
    let cache_dir = dirs.cache_dir();
    std::fs::create_dir_all(cache_dir).context("setting up the cache directory")?;
    Ok(cache_dir.into())
}
