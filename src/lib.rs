// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![cfg_attr(feature = "parquet", doc = include_str!("../README.md"))]

use std::cell::{RefCell, RefMut};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
#[cfg(feature = "arrow")]
use arrow::array::StructArray;
use rayon::prelude::*;
use thread_local::ThreadLocal;
#[cfg(feature = "arrow")]
pub use arrow;

#[cfg(feature = "csv")]
mod csv;
#[cfg(feature = "csv")]
pub use csv::*;

#[cfg(feature = "arrow-ipc")]
mod ipc;
#[cfg(feature = "arrow-ipc")]
pub use ipc::*;

#[cfg(feature = "parquet")]
mod parquet_;
#[cfg(feature = "parquet")]
pub use parquet_::*;

mod partitioned;
pub use partitioned::*;

#[cfg(feature = "zstd")]
mod zstd;
#[cfg(feature = "zstd")]
pub use zstd::*;

#[cfg(feature = "arrow")]
#[allow(clippy::len_without_is_empty)]
pub trait StructArrayBuilder {
    /// Number of rows currently in the buffer (not capacity)
    fn len(&self) -> usize;
    /// Number of bytes currently in the buffer (not capacity)
    fn buffer_size(&self) -> usize;
    fn finish(&mut self) -> Result<StructArray>;
}

/// Writes a set of files (called tables here) to a directory.
pub struct ParallelDatasetWriter<W: TableWriter + Send> {
    num_files: AtomicU64,
    schema: W::Schema,
    path: PathBuf,
    writers: ThreadLocal<RefCell<W>>,
    pub config: W::Config,
}

impl<W: TableWriter<Schema = ()> + Send> ParallelDatasetWriter<W>
where
    W::Config: Default,
{
    pub fn new(path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        Ok(ParallelDatasetWriter {
            num_files: AtomicU64::new(0),
            schema: (),
            path,
            writers: ThreadLocal::new(),
            config: W::Config::default(),
        })
    }
}

impl<W: TableWriter + Send> ParallelDatasetWriter<W>
where
    W::Config: Default,
{
    pub fn with_schema(path: PathBuf, schema: W::Schema) -> Result<Self> {
        std::fs::create_dir_all(&path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        Ok(ParallelDatasetWriter {
            num_files: AtomicU64::new(0),
            schema,
            path,
            writers: ThreadLocal::new(),
            config: W::Config::default(),
        })
    }

    fn get_new_seq_writer(&self) -> Result<RefCell<W>> {
        let path = self
            .path
            .join(self.num_files.fetch_add(1, Ordering::Relaxed).to_string());
        Ok(RefCell::new(W::new(
            path,
            self.schema.clone(),
            self.config.clone(),
        )?))
    }

    /// Returns a new sequential writer.
    ///
    /// # Panics
    ///
    /// When called from a thread holding another reference to a sequential writer
    /// of this dataset.
    pub fn get_thread_writer(&self) -> Result<RefMut<W>> {
        self.writers
            .get_or_try(|| self.get_new_seq_writer())
            .map(|writer| writer.borrow_mut())
    }

    /// Flushes all underlying writers
    pub fn flush(&mut self) -> Result<()> {
        self.writers
            .iter_mut()
            .collect::<Vec<_>>()
            .into_par_iter()
            .map(|writer| writer.get_mut().flush())
            .collect::<Result<Vec<()>>>()
            .map(|_: Vec<()>| ())
    }

    /// Closes all underlying writers
    pub fn close(mut self) -> Result<Vec<W::CloseResult>> {
        let mut tmp = ThreadLocal::new();
        std::mem::swap(&mut tmp, &mut self.writers);
        tmp.into_iter()
            .collect::<Vec<_>>()
            .into_par_iter()
            .map(|writer| writer.into_inner().close())
            .collect()
    }
}

impl<W: TableWriter + Send> Drop for ParallelDatasetWriter<W> {
    fn drop(&mut self) {
        let mut tmp = ThreadLocal::new();
        std::mem::swap(&mut tmp, &mut self.writers);
        tmp.into_iter()
            .collect::<Vec<_>>()
            .into_par_iter()
            .try_for_each(|writer| writer.into_inner().close().map(|_| ()))
            .expect("Could not close ParallelDatasetWriter");
    }
}

pub trait TableWriter {
    type Schema: Clone;
    type CloseResult: Send;
    type Config: Clone;

    fn new(path: PathBuf, schema: Self::Schema, config: Self::Config) -> Result<Self>
    where
        Self: Sized;

    /// Calls `.into()` on the internal builder, and writes its result to disk.
    fn flush(&mut self) -> Result<()>;

    fn close(self) -> Result<Self::CloseResult>;
}
