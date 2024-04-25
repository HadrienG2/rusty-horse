//! Disk cache of the dataset

use crate::Result;
use anyhow::Context;
use directories::ProjectDirs;
use std::path::Path;

/// Create the cache directory if it doesn't exist, and return its location
fn location() -> Result<Box<Path>> {
    let dirs = ProjectDirs::from("", "", env!("CARGO_PKG_NAME"))
        .context("determining the cache's location")?;
    let cache_dir = dirs.cache_dir();
    std::fs::create_dir_all(cache_dir).context("setting up the cache directory")?;
    Ok(cache_dir.into())
}
