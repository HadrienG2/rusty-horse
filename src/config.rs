//! Processing pipeline configuration

use crate::{languages::LanguageInfo, Args, Year};
use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

/// Final process configuration
///
/// This is the result of combining digested [`Args`] with language-specific
/// considerations. Please refer to [`Args`] to know more about common fields.
#[allow(missing_docs)]
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct Config {
    /// Truth that capitalized words should be removed from the dataset
    pub strip_capitalized: bool,
    pub min_year: Year,
    pub min_matches: NonZeroU64,
    pub min_books: NonZeroU64,
    pub memory_chunk: NonZeroUsize,
    pub storage_chunk: NonZeroUsize,
    pub max_outputs: Option<NonZeroUsize>,
    pub sort_by_popularity: bool,
}
//
impl Config {
    /// Determine process configuration from initialization products
    pub fn new(args: Args, language: LanguageInfo) -> Arc<Self> {
        let min_year = args.min_year();
        let storage_chunk = args.storage_chunk();
        let Args {
            language: _,
            strip_odd_capitalized,
            min_year: _,
            min_matches,
            min_books,
            memory_chunk,
            storage_chunk: _,
            max_outputs,
            sort_by_popularity,
        } = args;
        Arc::new(Self {
            strip_capitalized: strip_odd_capitalized && language.should_strip_capitalized,
            min_year,
            min_matches,
            min_books,
            memory_chunk,
            storage_chunk,
            max_outputs,
            sort_by_popularity,
        })
    }
}
