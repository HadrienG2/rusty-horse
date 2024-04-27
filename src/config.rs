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
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Config {
    /// Short language name
    pub language_id: Box<str>,

    /// Subset of the configuration that affects which data is loaded/kept
    pub input: InputConfig,

    // Other fields have the same meaning as in Args
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
            language_id: language.short_name.into(),
            input: InputConfig {
                strip_capitalized: strip_capitalized.unwrap_or(language.should_strip_capitalized),
                min_year,
                min_matches,
                min_books,
            },
            memory_chunk,
            mem_chunks_per_storage_chunk,
            max_outputs,
            sort_by_popularity,
        })
    }

    /// Rebuild with a different input configuration
    pub fn with_input_config(&self, input: InputConfig) -> Arc<Self> {
        Arc::new(Self {
            input,
            ..self.clone()
        })
    }

    /// Storage chunk size
    pub fn storage_chunk(&self) -> usize {
        self.memory_chunk.get() * self.mem_chunks_per_storage_chunk.get()
    }
}

/// Subset of the configuration that affects which data is loaded/kept
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct InputConfig {
    /// Truth that capitalized words should be removed from the dataset
    pub strip_capitalized: bool,

    // Other fields have the same meaning as in Args
    pub min_year: Year,
    pub min_matches: NonZeroU64,
    pub min_books: NonZeroU64,
}
//
impl InputConfig {
    /// Truth that a cache constructed with this input configuration can
    /// accomodate another app input configuration
    pub fn includes(&self, other: &Self) -> bool {
        (!self.strip_capitalized || other.strip_capitalized)
            && (self.min_year <= other.min_year)
            && (self.min_matches <= other.min_matches)
            && (self.min_books <= other.min_books)
    }

    /// Common superset that encompasses two input configurations
    pub fn common_superset(&self, other: &InputConfig) -> Self {
        Self {
            strip_capitalized: self.strip_capitalized && other.strip_capitalized,
            min_year: self.min_year.min(other.min_year),
            min_matches: self.min_matches.min(other.min_matches),
            min_books: self.min_books.min(other.min_books),
        }
    }
}
