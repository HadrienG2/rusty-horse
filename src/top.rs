//! Select the top ngrams from the final aggregated data set

use crate::{config::Config, dataset::Dataset, stats::NgramStats};
use rayon::prelude::*;
use std::{collections::BinaryHeap, num::NonZeroUsize};

/// Pick the top ngrams from the aggregated dataset
pub fn pick_top_ngrams<'dataset>(
    config: &Config,
    dataset: &'dataset Dataset,
) -> Vec<&'dataset str> {
    // Iterate over blocks of case equivalence classes
    let stats_and_ngrams = (dataset.blocks().par_iter()).flat_map(|block| {
        // For each case equivalence class, determine the overall usage
        // statistics and most frequent casing over the time period of interest.
        (block.case_classes())
            .filter_map(|class| {
                let mut case_stats: Option<(NgramStats, &str, NgramStats)> = None;
                'casings: for casing in class.ngrams() {
                    // Compute usage statistics for this casing
                    let ngram_stats = (casing.years())
                        .take_while(|data| data.year >= config.min_year)
                        .fold(None, |acc: Option<NgramStats>, year| {
                            if let Some(mut acc) = acc {
                                acc.add_year(year);
                                Some(acc)
                            } else {
                                Some(NgramStats::from(year))
                            }
                        });

                    // Reject casings that don't meet our popularity criteria
                    let Some(ngram_stats) = ngram_stats else {
                        continue 'casings;
                    };
                    if ngram_stats.match_count() < config.min_matches
                        || ngram_stats.min_volume_count() < config.min_books
                    {
                        continue 'casings;
                    }

                    // Keep track of the most frequent casing
                    if let Some((total_stats, top_ngram, top_stats)) = &mut case_stats {
                        total_stats.merge_equivalent(ngram_stats);
                        if ngram_stats > *top_stats {
                            *top_ngram = casing.ngram();
                            *top_stats = ngram_stats;
                        }
                    } else {
                        case_stats = Some((ngram_stats, casing.ngram(), ngram_stats));
                    }
                }
                case_stats.map(|(total_stats, top_ngram, _top_stats)| (total_stats, top_ngram))
            })
            .collect::<Vec<_>>()
    });

    // If the output is unbounded and unsorted, just dump the ngrams quickly
    if config.max_outputs.is_none() && !config.sort_by_popularity {
        return stats_and_ngrams.map(|(_stats, ngram)| ngram).collect();
    }

    // Otherwise, sort the ngrams by popularity, picking the most frequent ones
    // if requested
    let mut result = if let Some(max_outputs) = config.max_outputs {
        Vec::with_capacity(max_outputs.get())
    } else {
        Vec::new()
    };
    let mut stats_and_ngrams = stats_and_ngrams.collect::<BinaryHeap<_>>();
    while let Some((_stats, ngram)) = stats_and_ngrams.pop() {
        result.push(ngram);
        if result.len() == config.max_outputs.map_or(usize::MAX, NonZeroUsize::get) {
            return result;
        }
    }
    result
}
