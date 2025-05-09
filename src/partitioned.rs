// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::ffi::OsString;
use std::num::NonZeroU16;
use std::path::PathBuf;

use anyhow::{ensure, Context, Result};
use rayon::prelude::*;

use crate::TableWriter;

/// Alias of [`U16PartitionedTableWriter`] for backward compatibility
pub type PartitionedTableWriter<PartitionWriter> = U16PartitionedTableWriter<PartitionWriter>;

/// Wraps `N` [`TableWriter`] in such a way that they each write to `base/0/x.parquet`,
/// ..., `base/N-1/x.parquet` instead of `base/x.parquet`.
///
/// This allows Hive partitioning while writing with multiple threads (`x` is the
/// thread id in the example above).
///
/// If `num_partitions` is `None`, disables partitioning.
pub struct U16PartitionedTableWriter<PartitionWriter: TableWriter + Send> {
    partition_writers: Vec<PartitionWriter>,
}

impl<PartitionWriter: TableWriter + Send> TableWriter
    for U16PartitionedTableWriter<PartitionWriter>
{
    /// `(partition_column, num_partitions, underlying_schema)`
    type Schema = (String, Option<NonZeroU16>, PartitionWriter::Schema);
    type CloseResult = Vec<PartitionWriter::CloseResult>;
    type Config = PartitionWriter::Config;

    fn new(
        mut path: PathBuf,
        (partition_column, num_partitions, schema): Self::Schema,
        config: Self::Config,
    ) -> Result<Self> {
        // Remove the last part of the path (the thread id), so we can insert the
        // partition number between the base path and the thread id.
        let thread_id = path.file_name().map(|p| p.to_owned());
        ensure!(
            path.pop(),
            "Unexpected root path for partitioned writer: {}",
            path.display()
        );
        let thread_id = thread_id.unwrap();
        Ok(U16PartitionedTableWriter {
            partition_writers: (0..num_partitions.map(NonZeroU16::get).unwrap_or(1))
                .map(|partition_id| {
                    let partition_path = if num_partitions.is_some() {
                        path.join(format!("{}={}", partition_column, partition_id))
                    } else {
                        // Partitioning disabled
                        path.to_owned()
                    };
                    std::fs::create_dir_all(&partition_path).with_context(|| {
                        format!("Could not create {}", partition_path.display())
                    })?;
                    PartitionWriter::new(
                        partition_path.join(&thread_id),
                        schema.clone(),
                        config.clone(),
                    )
                })
                .collect::<Result<_>>()?,
        })
    }

    fn flush(&mut self) -> Result<()> {
        self.partition_writers
            .par_iter_mut()
            .try_for_each(|writer| writer.flush())
    }

    fn close(self) -> Result<Self::CloseResult> {
        self.partition_writers
            .into_par_iter()
            .map(|writer| writer.close())
            .collect()
    }
}

impl<PartitionWriter: TableWriter + Send> U16PartitionedTableWriter<PartitionWriter> {
    pub fn partitions(&mut self) -> &mut [PartitionWriter] {
        &mut self.partition_writers
    }
}

/// Wraps a set of [`TableWriter`] in such a way that they each write to a different
/// `base/<partition_key>/x.parquet` instead of `base/x.parquet`, where `<partition_key>`
/// is a UTF8 column.
///
/// This allows Hive partitioning while writing with multiple threads (`x` is the
/// thread id in the example above).
pub struct Utf8PartitionedTableWriter<PartitionWriter: TableWriter + Send> {
    path: PathBuf,
    partition_column: String,
    schema: PartitionWriter::Schema,
    config: PartitionWriter::Config,
    thread_id: OsString,
    partition_writers: HashMap<String, PartitionWriter>,
}

impl<PartitionWriter: TableWriter + Send> TableWriter
    for Utf8PartitionedTableWriter<PartitionWriter>
{
    /// `(partition_column, num_partitions, underlying_schema)`
    type Schema = (String, PartitionWriter::Schema);
    type CloseResult = Vec<PartitionWriter::CloseResult>;
    type Config = PartitionWriter::Config;

    fn new(
        mut path: PathBuf,
        (partition_column, schema): Self::Schema,
        config: Self::Config,
    ) -> Result<Self> {
        // Remove the last part of the path (the thread id), so we can insert the
        // partition number between the base path and the thread id.
        let thread_id = path.file_name().map(|p| p.to_owned());
        ensure!(
            path.pop(),
            "Unexpected root path for partitioned writer: {}",
            path.display()
        );
        let thread_id = thread_id.unwrap();
        Ok(Utf8PartitionedTableWriter {
            path,
            partition_column,
            schema,
            config,
            thread_id,
            partition_writers: HashMap::new(),
        })
    }

    fn flush(&mut self) -> Result<()> {
        self.partition_writers
            .par_iter_mut()
            .try_for_each(|(_partitiong_key, writer)| writer.flush())
    }

    fn close(self) -> Result<Self::CloseResult> {
        self.partition_writers
            .into_par_iter()
            .map(|(_partitiong_key, writer)| writer.close())
            .collect()
    }
}

impl<PartitionWriter: TableWriter + Send> Utf8PartitionedTableWriter<PartitionWriter> {
    pub fn partition(&mut self, partition_key: String) -> Result<&mut PartitionWriter> {
        match self.partition_writers.entry(partition_key) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let partition_path = self
                    .path
                    .join(format!("{}={}", self.partition_column, entry.key()));
                std::fs::create_dir_all(&partition_path)
                    .with_context(|| format!("Could not create {}", partition_path.display()))?;
                Ok(entry.insert(PartitionWriter::new(
                    partition_path.join(&self.thread_id),
                    self.schema.clone(),
                    self.config.clone(),
                )?))
            }
        }
    }
    pub fn partitions(&mut self) -> &mut HashMap<String, PartitionWriter> {
        &mut self.partition_writers
    }
}
