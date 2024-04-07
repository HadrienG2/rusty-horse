//! Select the top n-grams from the final aggregated data set

use crate::{config::Config, progress::ProgressReport, stats::FileStats, Ngram};
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, VecDeque},
};

/// Pick the top n-grams from the aggregated dataset
pub fn pick_top_ngrams(
    config: &Config,
    full_stats: FileStats,
    report: &ProgressReport,
) -> Vec<Ngram> {
    // Sort n-grams by frequency and pick the most frequent ones if requested
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

    // Don't waste time ordering the output unless we're asked to
    if config.sort_by_popularity {
        let mut ngrams_by_decreasing_stats = VecDeque::with_capacity(top_entries.len());
        while let Some((_, ngram)) = top_entries.pop() {
            ngrams_by_decreasing_stats.push_front(ngram);
        }
        ngrams_by_decreasing_stats.into()
    } else {
        top_entries
            .into_iter()
            .map(|(_stats, ngram)| ngram)
            .collect()
    }
}