//! Disk cache of the dataset

use super::Dataset;
use crate::{
    config::{Config, InputConfig},
    progress::{ProgressConfig, ProgressReport, Work},
    Result, Year, YearMatchCount, YearVolumeCount,
};
use anyhow::Context;
use arrow::{
    array::{
        ArrayRef, Int16Array, Int16Builder, MapArray, MapBuilder, MapFieldNames, RecordBatch,
        StringArray, StringBuilder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
    },
    datatypes::{DataType, Field, Schema},
};
use directories::ProjectDirs;
use futures::{FutureExt, StreamExt};
use parquet::{
    arrow::{
        async_reader::ParquetRecordBatchStream, AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
    },
    basic::{Compression, Encoding},
    file::properties::{WriterProperties, WriterPropertiesBuilder, WriterVersion},
};
use std::{
    collections::VecDeque,
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

/// Number of RecordBatches that can be queued before the task that generates
/// them is interrupted
const STORAGE_CHUNK_BUFFERING: usize = 3;

/// Load data from the cache or return the config for TSV loading
///
/// Loading data from TSV is very slow, and we want to avoid doing so. Therefore
/// we keep an on-disk cache and use it whenever feasible. At the same time we
/// don't want this on-disk cache to grow unnecessarily large, nor do we want to
/// expend a lot of RAM and CPU time into creating it and writing it down when
/// we do need to load data from TSV.
///
/// The way we resolve this tradeoff is that we do apply input filters to TSV
/// data before it is cached, but when the cache needs to be recreated because a
/// laxer input filter has been applied, we take the minimal superset of the
/// current input filter and the previous cache's input filter. This way, the
/// cache input filter is a superset of all input filters that have ever be
/// used, and thus even if the user keeps alternating between lax and strict
/// input filter configs, we should always be able to reuse the cache.
pub async fn try_load(config: Arc<Config>) -> Result<LoadOutcome> {
    // Set up cache directory
    let cache_dir = cache_dir(&config).context("looking up the cache's location")?;

    // From this point on, if we fail due to an unusable cache, in a manner that
    // seems recoverable by recreating a new cache, we'll ask the client to
    // recreate a cache with this input configuration.
    let mut tsv_input_config = config.input;
    'cache_miss: {
        // Make sure we do have a cache
        if !cache_dir.try_exists().context("probing cache existence")? {
            break 'cache_miss;
        }

        // Read the input configuration that the cache was created with
        let cache_config = CacheConfig::new(&cache_dir);
        let config_json = fs::read(&cache_config.config_path)
            .await
            .context("reading cache configuration")?;
        let Ok(cache_input_config) = serde_json::from_slice::<InputConfig>(&config_json) else {
            break 'cache_miss;
        };

        // From this point on, if the cache must be recreated, we'll use the
        // common superset of the old and new cache input configuration
        tsv_input_config = cache_input_config.common_superset(&config.input);

        // Check if the cache input configuration is broad enough
        if !cache_input_config.includes(&config.input) {
            break 'cache_miss;
        }

        // Now, open the case classes file...
        let case_classes_stream = ParquetRecordBatchStreamBuilder::new(
            File::open(&cache_config.case_classes_path)
                .await
                .context("opening case classes file")?,
        )
        .await
        .context("setting up case classes stream builder")?
        .with_batch_size(config.storage_chunk())
        // FIXME: Would like to try row filters for early rejection of capitalized
        //        words, but this is incompatible with the current data_end relation
        //        scheme because I need the data_end of row N-1 to know the start of
        //        the data span associated with row N. Will need to make structured
        //        Arrow data work first.
        .build()
        .context("building case classes stream")?;
        if *case_classes_stream.schema() != cache_config.case_classes_schema {
            break 'cache_miss;
        }

        // ...and the yearly data file
        let year_data_stream = ParquetRecordBatchStreamBuilder::new(
            File::open(&cache_config.year_data_path)
                .await
                .context("opening yearly data file")?,
        )
        .await
        .context("setting up yearly data stream builder")?
        .with_batch_size(config.storage_chunk())
        // FIXME: Would like to try row filters for early rejection of old years,
        //        but this si incompatible with the current data_end relation scheme
        //        as it changes row indices. Will need to make structured Arrow data
        //        work first.
        .build()
        .context("building yearly data stream")?;
        if *year_data_stream.schema() != cache_config.year_data_schema {
            break 'cache_miss;
        }

        // Start reading data from the cache asynchronously
        let (storage_blocks, reader) =
            start_reading_dataset(config, case_classes_stream, year_data_stream);
        return Ok(LoadOutcome::Hit {
            storage_blocks,
            reader,
        });
    }

    // If the cache is not usable, request the client to recreate one
    Ok(LoadOutcome::Miss(tsv_input_config))
}
//
/// Outcome of trying to load data from cache
#[derive(Debug)]
pub enum LoadOutcome {
    /// Could reuse an existing cache
    Hit {
        /// Stream of storage blocks coming from the cache
        storage_blocks: Receiver<Dataset>,

        /// Asynchronous task that reads out the storage blocks
        reader: JoinHandle<Result<()>>,
    },

    /// No suitable
    ///
    /// In this case, the input configuration that should be used when reloading
    /// the TSV data is provided. This config may be laxer than the current
    /// application input configuration, and should therefore not be used when
    /// computing the actual top ngrams at the end.
    Miss(InputConfig),
}

/// Save the current configuration and dataset into the cache directory
pub async fn save(
    config: Arc<Config>,
    dataset: Arc<Dataset>,
    report: ProgressReport,
) -> Result<()> {
    // Set up the application's cache directory & find language cache location
    let cache_dir = cache_dir(&config).context("looking up the language cache's location")?;

    // Use a temporary directory while in the process of saving the cache
    let temp_dir = tempfile::tempdir().context("creating a temporary directory for the cache")?;
    let cache_config = CacheConfig::new(temp_dir.path());

    // Save the input configuration that the cache was created with
    let config_json = serde_json::to_vec_pretty(&config.input)
        .context("converting app input configuration to JSON")?;
    fs::write(&cache_config.config_path, &config_json)
        .await
        .context("saving app input configuration to disk")?;

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
        start_making_record_batches(config, cache_config, dataset);
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
            Field::new(YEAR_DATA_YEARS, DataType::Int16, true),
            Field::new(YEAR_DATA_MATCH_COUNTS, DataType::UInt64, true),
            Field::new(YEAR_DATA_VOLUME_COUNTS, DataType::UInt32, true),
        ]));
        let case_classes_path = root_path.join("case_classes.parquet").into();
        let case_classes_schema = Arc::new(Schema::new(vec![Field::new_map(
            CASE_CLASSES_MAP,
            CASE_CLASSES_ENTRY,
            Field::new(CASE_CLASSES_NGRAMS, DataType::Utf8, false),
            // FIXME: Figure out how to use non-nullable data here
            Field::new(CASE_CLASSES_DATA_ENDS, DataType::UInt64, true),
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

// These column names must be kept in sync between code that uses them
const CASE_CLASSES_MAP: &str = "case_classes";
const CASE_CLASSES_ENTRY: &str = "ngram_to_data_end";
const CASE_CLASSES_NGRAMS: &str = "ngrams";
const CASE_CLASSES_DATA_ENDS: &str = "data_ends";
const YEAR_DATA_YEARS: &str = "years";
const YEAR_DATA_MATCH_COUNTS: &str = "match_counts";
const YEAR_DATA_VOLUME_COUNTS: &str = "volume_counts";

/// Asynchronously read out dataset storage blocks from the cache
fn start_reading_dataset(
    app_config: Arc<Config>,
    mut case_classes_stream: ParquetRecordBatchStream<File>,
    mut year_data_stream: ParquetRecordBatchStream<File>,
) -> (Receiver<Dataset>, JoinHandle<Result<()>>) {
    let (sender, receiver) = mpsc::channel(STORAGE_CHUNK_BUFFERING);
    let join_handle = tokio::task::spawn(async move {
        // General logic for recovering Rust offsets from arrow
        let recover_offsets = |offsets: &[i32]| -> Result<Box<[usize]>> {
            offsets
                .iter()
                .skip(1)
                .map(|offset| {
                    usize::try_from(*offset)
                        .context("asserting that arrow offsets are usize in disguise")
                })
                .collect::<Result<_>>()
        };

        // Yearly data buffering/chunking state
        let mut last_data_end = 0;
        let mut years = VecDeque::new();
        let mut match_counts = VecDeque::new();
        let mut volume_counts = VecDeque::new();

        // Read case equivalence classes, storage block by storage block
        //
        // FIXME remove
        #[allow(clippy::never_loop)]
        while let Some(case_classes) = case_classes_stream.next().await {
            // Pull one batch of case classes
            let case_classes = case_classes.context("reading case classes chunk")?;
            let case_classes: &MapArray = case_classes
                .column_by_name(CASE_CLASSES_MAP)
                .context("accessing case classes map")?
                .as_any()
                .downcast_ref()
                .context("re-typing map column")?;
            let case_class_ends: Box<[usize]> = recover_offsets(case_classes.value_offsets())
                .context("recovering case class offsets")?;

            // Recover ngram strings
            let ngrams_array: &StringArray = case_classes
                .keys()
                .as_any()
                .downcast_ref()
                .context("re-typing ngrams column")?;
            let ngrams: Box<str> = std::str::from_utf8(ngrams_array.value_data())
                .context("decoding ngram utf8")?
                .into();
            let ngram_str_ends: Box<[usize]> = recover_offsets(ngrams_array.value_offsets())
                .context("recovering ngram offsets")?;

            // Recover data offsets, relative to start of storage block
            let ngram_data_ends: Box<[usize]> =
                case_classes
                    .values()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .context("re-typing data_ends column")?
                    .iter()
                    .flatten()
                    .map(|x| {
                        Ok(usize::try_from(x).context("recovering ngram data offsets")?
                            - last_data_end)
                    })
                    .collect::<Result<_>>()?;

            // Read all the yearly data needed to cover these case classes
            let data_len = ngram_data_ends.last().copied().unwrap_or(0);
            while years.len() < data_len {
                // Load a block of yearly data from storage
                let year_data = year_data_stream
                    .next()
                    .await
                    .context("reading yearly data chunk")?
                    .context(
                        "yearly data that's advertised by case class data should be present",
                    )?;

                // Recover years in which data was recorded
                years.extend(
                    year_data
                        .column_by_name(YEAR_DATA_YEARS)
                        .context("accessing years")?
                        .as_any()
                        .downcast_ref::<Int16Array>()
                        .context("re-typing years")?
                        .iter()
                        .flatten(),
                );

                // Recover number of matches
                match_counts.extend(
                    year_data
                        .column_by_name(YEAR_DATA_MATCH_COUNTS)
                        .context("accessing match_counts")?
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .context("re-typing match_counts")?
                        .iter()
                        .flatten(),
                );

                // Recover number of matching books
                volume_counts.extend(
                    year_data
                        .column_by_name(YEAR_DATA_VOLUME_COUNTS)
                        .context("accessing volume_counts")?
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .context("re-typing volume_counts")?
                        .iter()
                        .flatten(),
                );
            }

            // Extract the yearly data needed by these case classes
            let years: Box<[Year]> = years.drain(..data_len).collect();
            let match_counts: Box<[YearMatchCount]> = match_counts
                .drain(..data_len)
                .map(|matches| {
                    YearMatchCount::new(matches).context("asserting match count is nonzero")
                })
                .collect::<Result<_>>()?;
            let match_counts: Box<[YearVolumeCount]> = volume_counts
                .drain(..data_len)
                .map(|volumes| {
                    YearVolumeCount::new(volumes).context("asserting volume count is nonzero")
                })
                .collect::<Result<_>>()?;
            last_data_end += data_len;

            // TODO: Slice up storage block into memory blocks
            // TODO: Send dataset to main thread
            // TODO: Remove allow(clippy::never_loop) above once done
            todo!()
        }
        Ok(())
    });
    (receiver, join_handle)
}

/// Convert the dataset to RecordBatches at storage block granularity
fn start_making_record_batches(
    app_config: Arc<Config>,
    cache_config: CacheConfig,
    dataset: Arc<Dataset>,
) -> (Receiver<RecordBatches>, JoinHandle<Result<()>>) {
    let (sender, receiver) = mpsc::channel(STORAGE_CHUNK_BUFFERING);
    let join_handle = tokio::task::spawn_blocking(move || {
        // For each storage chunk in the dataset...
        let mut year_data_len = 0;
        for storage_chunk in dataset
            .blocks()
            .chunks(app_config.mem_chunks_per_storage_chunk.get())
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
                    entry: CASE_CLASSES_ENTRY.into(),
                    key: CASE_CLASSES_NGRAMS.into(),
                    value: CASE_CLASSES_DATA_ENDS.into(),
                }),
                StringBuilder::with_capacity(num_ngrams, num_ngram_str_bytes),
                UInt64Builder::with_capacity(num_ngrams),
                app_config.storage_chunk(),
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
fn cache_dir(config: &Config) -> Result<Box<Path>> {
    let dirs = ProjectDirs::from("", "", env!("CARGO_PKG_NAME"))
        .context("determining the cache's location")?;
    let cache_dir = dirs.cache_dir();
    std::fs::create_dir_all(cache_dir).context("setting up the cache directory")?;
    Ok(cache_dir.join(&*config.language_id).into_boxed_path())
}
