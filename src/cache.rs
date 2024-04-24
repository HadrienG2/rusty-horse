//! Apache Arrow/Parquet disk cache of the Google Ngram dataset
//!
//! The dataset is provided as gzipped TSV, which is a needlessly inefficient
//! format. Instead, our disk cache will use an Apache Parquet based format
//! tailored to our needs :
//!
//! - Ngrams are grouped by case equivalence classes
//! - Yearly data does not include unnecessary redundant ngram copies
//! - Everything is sorted by decreasing year and popularity to enable early
//!   exit when some user cutoff is reached.

// TODO: Finish dataset module, then implement this one
