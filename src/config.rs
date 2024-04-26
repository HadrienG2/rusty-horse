//! Processing pipeline configuration

use crate::{languages::LanguageInfo, Args, Year};
use serde::{Deserialize, Serialize};
use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

/// Final process configuration
///
/// This is the result of combining digested [`Args`] with language-specific
/// considerations. Please refer to [`Args`] to know more about common fields.
#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Config {
    /// Short name of the language in use
    pub language_name: Box<str>,

    /// Truth that capitalized words should be removed from the dataset
    pub strip_capitalized: bool,

    // Other fields have the same meaning as in Args
    pub min_year: Year,
    pub min_matches: NonZeroU64,
    pub min_books: NonZeroU64,
    pub memory_chunk: NonZeroUsize,
    pub mem_chunks_per_storage_chunk: NonZeroUsize,
    pub max_outputs: Option<NonZeroUsize>,
    pub sort_by_popularity: bool,
}
//
impl Config {
    /// Determine process configuration from initialization products
    pub(crate) fn new(args: Args, language: LanguageInfo) -> Arc<Self> {
        let min_year = args.min_year();
        let mem_chunks_per_storage_chunk = args.mem_chunks_per_storage_chunk();
        let Args {
            language: _,
            strip_capitalized,
            min_year: _,
            min_matches,
            min_books,
            memory_chunk,
            storage_chunk: _,
            max_outputs,
            sort_by_popularity,
        } = args;
        Arc::new(Self {
            language_name: language.short_name.into(),
            strip_capitalized: strip_capitalized.unwrap_or(language.should_strip_capitalized),
            min_year,
            min_matches,
            min_books,
            memory_chunk,
            mem_chunks_per_storage_chunk,
            max_outputs,
            sort_by_popularity,
        })
    }
}
