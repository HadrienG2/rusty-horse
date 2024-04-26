//! Disk cache of the dataset

use super::Dataset;
use crate::{
    config::Config,
    progress::{ProgressConfig, ProgressReport, Work},
    Result,
};
use anyhow::Context;
use arrow::{
    array::{
        ArrayRef, Int16Builder, MapBuilder, MapFieldNames, RecordBatch, StringBuilder,
        UInt32Builder, UInt64Builder,
    },
    datatypes::{DataType, Field, Schema},
};
use directories::ProjectDirs;
use futures::FutureExt;
use parquet::{
    arrow::AsyncArrowWriter,
    basic::{Compression, Encoding},
    file::properties::{WriterProperties, WriterPropertiesBuilder, WriterVersion},
};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    fs::{self, File},
    process::Command,
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
};

/// Number of RecordBatches that can be queued before the thread that generates
/// them is interrupted
const RECORD_BATCH_BUFFERING: usize = 3;

/// Save the current configuration and dataset into the cache directory
pub async fn save(
    config: Arc<Config>,
    dataset: Arc<Dataset>,
    report: ProgressReport,
) -> Result<()> {
    // Set up the application's cache directory & find language cache location
    let cache_dir =
        cache_dir(&config.language_name).context("looking up the language cache's location")?;

    // Use a temporary directory while in the process of saving the cache
    let temp_dir = tempfile::tempdir().context("creating a temporary directory for the cache")?;
    let cache_config = CacheConfig::new(temp_dir.path());

    // Save the app configuration that the cache was created with
    let config_json =
        serde_json::to_vec_pretty(&*config).context("converting app configuration to JSON")?;
    fs::write(&cache_config.config_path, &config_json)
        .await
        .context("saving JSON app configuration to disk")?;

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
    let apply_common_properties_and_build = |builder: WriterPropertiesBuilder| {
        builder
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::LZ4_RAW)
            .build()
    };
    let mut year_data_writer = AsyncArrowWriter::try_new(
        File::create(&cache_config.year_data_path)
            .await
            .context("creating the yearly data file")?,
        cache_config.year_data_schema.clone(),
        Some(apply_common_properties_and_build(
            WriterProperties::builder()
                .set_column_encoding("years".into(), Encoding::DELTA_BINARY_PACKED)
                .set_column_encoding("match_counts".into(), Encoding::PLAIN)
                .set_column_encoding("volume_counts".into(), Encoding::PLAIN),
        )),
    )
    .context("preparing to write down yearly data")?;
    let mut case_classes_writer = AsyncArrowWriter::try_new(
        File::create(&cache_config.case_classes_path)
            .await
            .context("creating the case classes file")?,
        cache_config.case_classes_schema.clone(),
        Some(apply_common_properties_and_build(
            WriterProperties::builder().set_column_encoding(
                "case_classes.ngram_to_data_end.data_ends".into(),
                Encoding::DELTA_BINARY_PACKED,
            ),
        )),
    )
    .context("preparing to write down case classes")?;

    // Set up progress reporting
    let mem_chunks_per_storage_chunk = config.mem_chunks_per_storage_chunk.get();
    let num_storage_chunks = dataset
        .blocks()
        .len()
        .div_ceil(mem_chunks_per_storage_chunk);
    let save = report.add(
        "Saving data cache",
        ProgressConfig::new(Work::PercentSteps(num_storage_chunks)),
    );
    let mut reset_time = Instant::now();

    // Save the dataset using the user-requested chunk size
    let (mut batches, batch_maker_join) =
        start_making_record_batches(config, dataset, cache_config);
    while let Some(batches) = batches.recv().await {
        // Submit the writes to parquet
        let year_data_write = year_data_writer.write(&batches.year_data);
        let case_classes_write = case_classes_writer.write(&batches.case_classes);

        // Wait for both I/O operations and handle I/O errors
        futures::try_join!(year_data_write, case_classes_write)
            .context("submitting cache writes")?;

        // Track progress. Remaining time estimate is quite bad on this
        // operation because earlier iterations are slower than later
        // iterations, so it is a good idea to periodically reset the estimate.
        save.make_progress(1);
        if reset_time.elapsed() > Duration::from_secs(2) {
            save.reset_eta();
            reset_time = Instant::now();
        }
    }

    // Wait for the ongoing operations to finish
    futures::try_join!(
        batch_maker_join.map(|e| e.context("joining the batch-making thread")?),
        year_data_writer
            .close()
            .map(|e| e.context("closing the yearly data file")),
        case_classes_writer
            .close()
            .map(|e| e.context("closing the case classes file")),
    )?;

    // Move the successfully saved cache to its final location
    if cache_dir.exists() {
        tokio::fs::remove_dir_all(&cache_dir)
            .await
            .context("deleting old version of the cache")?;
    }
    let temp_dir = temp_dir.into_path();
    let status = Command::new("mv")
        .arg(temp_dir)
        .arg(&*cache_dir)
        .status()
        .await
        .context("moving cache to its final location")?;
    if !status.success() {
        anyhow::bail!("failed to move cache to its final location with {status}");
    }
    Ok(())
}

/// Setup common to reading and writing to a cache
struct CacheConfig {
    /// Location of the JSON config file
    config_path: Box<Path>,

    /// Location of the yearly data
    year_data_path: Box<Path>,

    /// Schema of the yearly data
    year_data_schema: Arc<Schema>,

    /// Location of the case equivalence classes
    case_classes_path: Box<Path>,

    /// Schema of the case equivalence classes
    case_classes_schema: Arc<Schema>,
}
//
impl CacheConfig {
    /// Given the cache directory's location, compute other info
    pub fn new(root_path: &Path) -> Self {
        let config_path = root_path.join("config.json").into();
        let year_data_path = root_path.join("year_data.parquet").into();
        let year_data_schema = Arc::new(Schema::new(vec![
            // FIXME: Figure out how to use non-nullable data here
            Field::new("years", DataType::Int16, true),
            Field::new("match_counts", DataType::UInt64, true),
            Field::new("volume_counts", DataType::UInt32, true),
        ]));
        let case_classes_path = root_path.join("case_classes.parquet").into();
        let case_classes_schema = Arc::new(Schema::new(vec![Field::new_map(
            "case_classes",
            "ngram_to_data_end",
            Field::new("ngrams", DataType::Utf8, false),
            // FIXME: Figure out how to use non-nullable data here
            Field::new("data_ends", DataType::UInt64, true),
            false,
            false,
        )]));
        Self {
            config_path,
            year_data_path,
            year_data_schema,
            case_classes_path,
            case_classes_schema,
        }
    }
}

/// Convert the dataset to RecordBatches at storage block granularity
fn start_making_record_batches(
    config: Arc<Config>,
    dataset: Arc<Dataset>,
    cache_config: CacheConfig,
) -> (Receiver<RecordBatches>, JoinHandle<Result<()>>) {
    let (sender, receiver) = mpsc::channel(RECORD_BATCH_BUFFERING);
    let join_handle = tokio::task::spawn_blocking(move || {
        let mem_chunks_per_storage_chunk = config.mem_chunks_per_storage_chunk.get();
        let case_classes_per_storage_chunk =
            mem_chunks_per_storage_chunk * config.memory_chunk.get();
        let mut year_data_len = 0;
        // For each storage chunk in the dataset...
        for storage_chunk in dataset
            .blocks()
            .chunks(config.mem_chunks_per_storage_chunk.get())
        {
            // Allocate storage buffers
            let num_ngrams = (storage_chunk.iter())
                .map(|mem_chunk| mem_chunk.ngram_str_ends.len())
                .sum();
            let num_ngram_str_bytes = (storage_chunk.iter())
                .map(|mem_chunk| mem_chunk.ngrams.len())
                .sum();
            let num_data_rows = (storage_chunk.iter())
                .map(|mem_chunk| mem_chunk.years.len())
                .sum();
            let mut case_classes = MapBuilder::with_capacity(
                Some(MapFieldNames {
                    entry: "ngram_to_data_end".into(),
                    key: "ngrams".into(),
                    value: "data_ends".into(),
                }),
                StringBuilder::with_capacity(num_ngrams, num_ngram_str_bytes),
                UInt64Builder::with_capacity(num_ngrams),
                case_classes_per_storage_chunk,
            );
            let mut years = Int16Builder::with_capacity(num_data_rows);
            let mut match_counts = UInt64Builder::with_capacity(num_data_rows);
            let mut volume_counts = UInt32Builder::with_capacity(num_data_rows);

            // Fill them with data
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
                    case_classes
                        .append(true)
                        .context("recording a case equivalence class")?;
                }
            }

            // Convert the collected data into RecordBatches and submit them to
            // the I/O thread for writing to disk
            let year_data = RecordBatch::try_new(
                cache_config.year_data_schema.clone(),
                vec![
                    Arc::new(years.finish()) as ArrayRef,
                    Arc::new(match_counts.finish()) as ArrayRef,
                    Arc::new(volume_counts.finish()) as ArrayRef,
                ],
            )
            .context("creating a yearly data batch")?;
            let case_classes = RecordBatch::try_new(
                cache_config.case_classes_schema.clone(),
                vec![Arc::new(case_classes.finish()) as ArrayRef],
            )
            .context("creating a case classes data batch")?;
            sender
                .blocking_send(RecordBatches {
                    year_data,
                    case_classes,
                })
                .context("sending data to the I/O thread")?;
        }
        Ok(())
    });
    (receiver, join_handle)
}

/// RecordBatches from a storage block, ready to be written to disk
struct RecordBatches {
    year_data: RecordBatch,
    case_classes: RecordBatch,
}

/// Create the cache directory if it doesn't exist, and return the location of
/// its subdirectory for a certain language of interest
fn cache_dir(language_name: &str) -> Result<Box<Path>> {
    let dirs = ProjectDirs::from("", "", env!("CARGO_PKG_NAME"))
        .context("determining the cache's location")?;
    let cache_dir = dirs.cache_dir();
    std::fs::create_dir_all(cache_dir).context("setting up the cache directory")?;
    Ok(cache_dir.join(language_name).into_boxed_path())
}
