//! Select the top ngrams from the final aggregated data set

use crate::{config::Config, progress::ProgressReport, stats::FileStats, Ngram};
use rayon::prelude::*;
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, VecDeque},
};

/// Pick the top ngrams from the aggregated dataset
pub fn pick_top_ngrams(
    config: &Config,
    full_stats: FileStats,
    report: &ProgressReport,
) -> Vec<Ngram> {
    // If the output is unbounded and unsorted, just dump the ngrams quickly
    if config.max_outputs.is_none() && !config.sort_by_popularity {
        return full_stats
            .into_par_iter()
            .map(|(_, case_stats)| {
                let (ngram, stats) = case_stats.collect();
                log::debug!("Keeping ngram {ngram} with statistics {:#?}", stats);
                ngram
            })
            .collect();
    }

    // Sort ngrams by popularity, picking the most frequent ones if requested
    report.start_sort(full_stats.len());
    let mut top_entries = if let Some(max_outputs) = config.max_outputs {
        BinaryHeap::with_capacity(max_outputs.get())
    } else {
        BinaryHeap::with_capacity(full_stats.len())
    };
    for (_, case_stats) in full_stats {
        let (top_ngram, total_stats) = case_stats.collect();
        top_entries.push((Reverse(total_stats), top_ngram));
        if let Some(max_outputs) = config.max_outputs {
            if top_entries.len() > max_outputs.get() {
                top_entries.pop();
            }
        }
        report.inc_sorted(1);
    }

    // Don't waste time producing a sorted output unless we're asked to do so
    if config.sort_by_popularity {
        let mut ngrams_by_decreasing_stats = VecDeque::with_capacity(top_entries.len());
        while let Some((stats, ngram)) = top_entries.pop() {
            log::debug!("Keeping ngram {ngram} with statistics {:#?}", stats.0);
            ngrams_by_decreasing_stats.push_front(ngram);
        }
        ngrams_by_decreasing_stats.into()
    } else {
        top_entries
            .into_iter()
            .map(|(stats, ngram)| {
                log::debug!("Keeping ngram {ngram} with statistics {:#?}", stats.0);
                ngram
            })
            .collect()
    }
}
