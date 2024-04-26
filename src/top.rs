//! Select the top ngrams from the final aggregated data set

use crate::{
    config::Config,
    dataset::{CaseClassView, Dataset},
    progress::{ProgressConfig, ProgressReport, Work},
    stats::NgramStats,
};
use rayon::prelude::*;
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, VecDeque},
    num::NonZeroUsize,
};

/// Pick the top ngrams from the aggregated dataset
pub fn pick_top_ngrams<'dataset>(
    config: &Config,
    dataset: &'dataset Dataset,
    report: &ProgressReport,
) -> Vec<&'dataset str> {
    // Map each case equivalence class into a tuple of global statistics over
    // all ngrams + most common ngrams, or discard the case equivalence class
    let stats = report.add(
        "Collecting usage statistics",
        ProgressConfig::new(Work::PercentSteps(dataset.blocks().len())),
    );
    let rev_stats_and_ngrams = (dataset.blocks().par_iter()).flat_map(|block| {
        let result = (block.case_classes())
            .filter_map(|class| case_class_filter_map(config, class))
            .collect::<Vec<_>>();
        stats.make_progress(1);
        result
    });

    // What happens next depends on the output configuration
    match (
        config.max_outputs.map(NonZeroUsize::get),
        config.sort_by_popularity,
    ) {
        // If there is no sorting and no limit, just dump the ngrams as-is
        (None, false) => rev_stats_and_ngrams.map(|(_stats, ngram)| ngram).collect(),

        // If there is sorting without a limit on the output size, sort the
        // ngrams then dump them
        (None, true) => {
            let mut sorted = rev_stats_and_ngrams.collect::<Vec<_>>();
            sorted.par_sort_unstable_by_key(|(rev_stats, _ngram)| *rev_stats);
            (sorted.into_par_iter())
                .map(|(_rev_stats, ngram)| ngram)
                .collect()
        }

        // If there is a limit, then...
        (Some(max_len), sort) => {
            // Find the top ngrams up to this limit
            let mut top_stats_and_ngrams = rev_stats_and_ngrams
                // First determine top ngrams on each thread using a min-heap...
                .fold(
                    || BinaryHeap::with_capacity(max_len),
                    |mut heap, (rev_stats, ngram)| {
                        heap.push((rev_stats, ngram));
                        if heap.len() > max_len {
                            heap.pop();
                        }
                        heap
                    },
                )
                // ...then merge thread results into a global result
                .reduce(BinaryHeap::new, |heap1, heap2| {
                    let (mut dst, mut src) = if heap1.len() >= heap2.len() {
                        (heap1, heap2)
                    } else {
                        (heap2, heap1)
                    };
                    while let Some(elem) = src.pop() {
                        dst.push(elem);
                        if dst.len() > max_len {
                            dst.pop();
                        }
                    }
                    dst
                });

            // If not asked to sort, return results in heap order
            if !sort {
                return top_stats_and_ngrams
                    .into_iter()
                    .map(|(_stats, ngram)| ngram)
                    .collect();
            }

            // Otherwise, collect the results in order of decreasing popularity
            // This will require an order reversal since we used a min-heap.
            let mut result = VecDeque::with_capacity(top_stats_and_ngrams.len());
            while let Some((_stats, ngram)) = top_stats_and_ngrams.pop() {
                result.push_front(ngram);
            }
            result.into()
        }
    }
}

/// Turn a case equivalence class from the dataset into a most common spelling +
/// statistics accumulated across all accepted spellings, or discard it
fn case_class_filter_map<'dataset>(
    config: &Config,
    class: CaseClassView<'dataset>,
) -> Option<(Reverse<NgramStats>, &'dataset str)> {
    let mut case_stats: Option<(NgramStats, &str, NgramStats)> = None;
    'casings: for casing in class.ngrams() {
        // Ignore capitalized ngrams if configured to do so
        if config.strip_capitalized
            && casing
                .ngram()
                .chars()
                .next()
                .expect("ngrams shouldn't be empty")
                .is_uppercase()
        {
            continue 'casings;
        }

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
        if !ngram_stats.is_acceptable(config) {
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
    // Prepare for sorting by descending statistics
    case_stats.map(|(total_stats, top_ngram, _top_stats)| (Reverse(total_stats), top_ngram))
}
