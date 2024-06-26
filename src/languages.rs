//! Supported Google Books Ngrams languages

use crate::{Args, Result};
use anyhow::Context;
use dialoguer::FuzzySelect;
use std::sync::OnceLock;

/// Pick a language as directed by the CLI arguments
pub fn pick(args: &Args) -> Result<LanguageInfo> {
    if let Some(language) = &args.language {
        get_language(language)
    } else {
        prompt_language()
    }
}

/// Get information about a language dictionary
fn get_language(short_name: &str) -> Result<LanguageInfo> {
    supported_languages()
        .iter()
        .find(|(_long_name, lang)| lang.short_name == short_name)
        .map(|(_long, lang)| *lang)
        .with_context(|| format!("failed to find user-requested language {short_name}"))
}

/// Ask the user to select a language dictionary
fn prompt_language() -> Result<LanguageInfo> {
    let languages = supported_languages();
    let language_names = languages
        .iter()
        .map(|(name, info)| format!("{name} ({})", info.short_name))
        .collect::<Vec<_>>();
    let language_idx = FuzzySelect::new()
        .with_prompt("Which dictionary should I use?")
        .items(&language_names)
        .default(0)
        .max_length(usize::MAX)
        .interact()?;
    Ok(languages[language_idx].1)
}

/// What we know about a language in the Google Books Ngrams dataset
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct LanguageInfo {
    /// Short name, as in dataset URLs
    pub short_name: &'static str,

    /// Valid word prefixes
    // NOTE: We only care about words because words are most memorable
    pub word_prefixes: &'static [Box<str>],

    /// Does it make sense to strip capitalized words?
    ///
    /// In most languages from the Google Books dataset, most words that start
    /// with a capital letter make an odd choice of passphrase building block.
    /// They are things like titles ("M.", "Ms."), proper nouns (which have a
    /// high potential of yielding an inflammatory result when combined with
    /// adjectives), Roman Numerals, isolated letters or acronyms.
    ///
    /// However, there are exceptions. For example, stripping capitalized words
    /// makes no sense in German, as you are basically throwing away every
    /// common noun. Hence there needs to be some metadata for this.
    pub should_strip_capitalized: bool,
}
//
impl LanguageInfo {
    /// Generate the URLs of the dataset files
    pub fn dataset_urls(&self) -> impl Iterator<Item = Box<str>> + '_ {
        self.word_prefixes.iter().map(move |word_prefix| {
            format!(
                // NOTE: For now, we only use 1-grams to keep the dataset small
                //       and avoid the extra complexity that comes with n-grams
                "http://storage.googleapis.com/books/ngrams/books/googlebooks-{}-all-1gram-20120701-{word_prefix}.gz",
                self.short_name,
            ).into()
        })
    }
}

/// What we know about each language that is supported by this program, keyed by
/// the language's human-readable name
fn supported_languages() -> &'static [(&'static str, LanguageInfo)] {
    static LAZY: OnceLock<Box<[(&'static str, LanguageInfo)]>> = OnceLock::new();
    LAZY.get_or_init(|| {
        [
            (
                "English",
                LanguageInfo {
                    short_name: "eng",
                    word_prefixes: latin_word_prefixes(),
                    should_strip_capitalized: true,
                },
            ),
            (
                "American English",
                LanguageInfo {
                    short_name: "eng-us",
                    word_prefixes: latin_word_prefixes(),
                    should_strip_capitalized: true,
                },
            ),
            (
                "British English",
                LanguageInfo {
                    short_name: "eng-gb",
                    word_prefixes: latin_word_prefixes(),
                    should_strip_capitalized: true,
                },
            ),
            (
                "English Fiction",
                LanguageInfo {
                    short_name: "eng-fiction",
                    word_prefixes: latin_word_prefixes(),
                    should_strip_capitalized: true,
                },
            ),
            // NOTE: To add Chinese support, need help from a speaker to tell
            //       what is a valid word prefix.
            (
                "French",
                LanguageInfo {
                    short_name: "fre",
                    word_prefixes: latin_word_prefixes(),
                    should_strip_capitalized: true,
                },
            ),
            (
                "German",
                LanguageInfo {
                    short_name: "ger",
                    word_prefixes: latin_word_prefixes(),
                    should_strip_capitalized: false,
                },
            ),
            // NOTE: To add Hebrew support, need help from a speaker to tell
            //       what is a valid word prefix.
            (
                "Italian",
                LanguageInfo {
                    short_name: "ita",
                    word_prefixes: latin_word_prefixes(),
                    should_strip_capitalized: true,
                },
            ),
            // NOTE: To add Russian support, need help from a speaker to tell
            //       what is a valid word prefix.
            (
                "Spanish",
                LanguageInfo {
                    short_name: "spa",
                    word_prefixes: latin_word_prefixes(),
                    should_strip_capitalized: true,
                },
            ),
        ]
        .into_iter()
        .collect()
    })
}

/// List of prefixes that are valid at the beginning of a word for all Latin
/// languages in the Google Books Ngrams v2 (20120701) dataset
fn latin_word_prefixes() -> &'static [Box<str>] {
    static LAZY: OnceLock<Box<[Box<str>]>> = OnceLock::new();
    LAZY.get_or_init(|| {
        ('a'..='z')
            .map(|c| c.to_string().into_boxed_str())
            .chain(std::iter::once("other".into()))
            .collect()
    })
}
