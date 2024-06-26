//! Processing pipeline configuration

use crate::{languages::LanguageInfo, Args, Year};
use std::{num::NonZeroUsize, sync::Arc};

/// Final process configuration
///
/// This is the result of combining digested [`Args`] with language-specific
/// considerations.
#[derive(Debug)]
pub struct Config {
    // TODO: "Bad word" exclusion mechanism
    //
    /// Whether capitalized n-grams should be ignored
    pub strip_capitalized: bool,

    /// Minimum accepted book publication year
    pub min_year: Year,

    /// Minimum accepted number of matches across of books
    pub min_matches: usize,

    /// Minimum accepted number of matching books
    pub min_books: usize,

    /// Maximal number of output n-grams
    pub max_outputs: Option<NonZeroUsize>,

    /// Sort output by decreasing popularity
    pub sort_by_popularity: bool,
}
//
impl Config {
    /// Determine process configuration from initialization products
    pub fn new(args: Args, language: LanguageInfo) -> Arc<Self> {
        let min_year = args.min_year();
        let Args {
            language: _,
            strip_odd_capitalized,
            min_year: _,
            min_matches,
            min_books,
            max_outputs,
            sort_by_popularity,
        } = args;
        Arc::new(Self {
            strip_capitalized: strip_odd_capitalized && language.should_strip_capitalized,
            min_year,
            min_matches,
            min_books,
            max_outputs,
            sort_by_popularity,
        })
    }
}
