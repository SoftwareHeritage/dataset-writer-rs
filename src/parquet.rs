// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};

use arrow::datatypes::Schema;
use parquet::arrow::ArrowWriter as ParquetWriter;
use parquet::file::properties::WriterProperties;
use parquet::format::FileMetaData;

use super::{StructArrayBuilder, TableWriter};

#[derive(Debug, Default, Clone)]
pub struct ParquetTableWriterConfig {
    /// Automatically flushes the builder to disk when its length (in number of rows)
    /// reaches the value.
    ///
    /// To avoid uneven row group sizes, this value plus the number of values added
    /// to the builder between calls to [`ParquetTableWriter::builder`] should be equal to
    /// [`max_row_group_size`](WriterProperties::max_row_group_size)
    /// (or a multiple of it).
    ///
    /// Defaults to [`max_row_group_size`](WriterProperties::max_row_group_size)
    /// if `None`.
    pub autoflush_row_group_len: Option<usize>,
    /// Automatically flushes the builder to disk when its size (in number of bytes
    /// in the arrays) reaches the value.
    ///
    /// Does not automatically flush on size if `None`
    pub autoflush_buffer_size: Option<usize>,
}

/// Writer to a .parquet file, usable with [`ParallelDatasetWriter`](super::ParallelDatasetWriter)
///
/// `Builder` should follow the pattern documented by
/// [`arrow::builder`](https://docs.rs/arrow/latest/arrow/array/builder/index.html)
pub struct ParquetTableWriter<Builder: Default + StructArrayBuilder> {
    base_path: PathBuf,
    /// See [`ParquetTableWriterConfig::autoflush_row_group_len`]
    pub autoflush_row_group_len: usize,
    /// See [`ParquetTableWriterConfig::autoflush_buffer_size`]
    pub autoflush_buffer_size: Option<usize>,
    schema: Arc<Schema>,
    properties: WriterProperties,
    file_writer: Option<(PathBuf, ParquetWriter<File>)>, // None only while initializing, and between .close() call and Drop
    num_written_files: u64,
    builder: Builder,
}

impl<Builder: Default + StructArrayBuilder> TableWriter for ParquetTableWriter<Builder> {
    type Schema = (Arc<Schema>, WriterProperties);
    type CloseResult = FileMetaData;
    type Config = ParquetTableWriterConfig;

    fn new(
        path: PathBuf,
        (schema, properties): Self::Schema,
        ParquetTableWriterConfig {
            autoflush_row_group_len,
            autoflush_buffer_size,
        }: Self::Config,
    ) -> Result<Self> {
        let base_path = path;

        let mut writer = ParquetTableWriter {
            base_path,
            // See above, we need to make sure the user does not write more than
            // `properties.max_row_group_size()` minus `autoflush_row_group_len` rows between
            // two calls to self.builder() to avoid uneven group sizes. This seems
            // like a safe ratio.
            autoflush_row_group_len: autoflush_row_group_len
                .unwrap_or(properties.max_row_group_size() * 9 / 10),
            autoflush_buffer_size,
            schema, properties,
            file_writer: None,
            num_written_files: 0,
            builder: Builder::default(),
        };
        writer.new_file_writer()?;
        Ok(writer)
    }

    fn flush(&mut self) -> Result<()> {
        // Get built array
        let struct_array = self.builder.finish()?;

        let (path, file_writer) = self
            .file_writer
            .as_mut()
            .expect("File writer is unexpectedly None");

        // Write it
        file_writer
            .write(&struct_array.into())
            .with_context(|| format!("Could not write to {}", path.display()))?;
        file_writer
            .flush()
            .with_context(|| format!("Could not flush to {}", path.display()))?;

        if file_writer.flushed_row_groups().len() >= (i16::MAX - 2).try_into().expect("i16 overflowed usize") {
            // Parquet does not support more than 32767 row groups per file, so we need to open a
            // new file.
            self.new_file_writer()?;
        }

        Ok(())
    }

    fn close(mut self) -> Result<FileMetaData> {
        self.flush()?;
        let (path, file_writer) = self.file_writer
            .take()
            .expect("File writer is unexpectedly None");
        file_writer
            .close()
            .with_context(|| format!("Could not close {}", path.display()))
    }
}

impl<Builder: Default + StructArrayBuilder> ParquetTableWriter<Builder> {
    fn new_file_writer(&mut self) -> Result<()> {
        // Close previous writer, if any.
        if let Some((path, file_writer)) = self.file_writer.take() {
            file_writer.close().with_context(|| format!("Could not close {}", path.display()))?;
            self.num_written_files += 1;
        }

        let mut path = if self.num_written_files == 0 {
            self.base_path.to_owned()
        } else {
            let mut file_name = self.base_path.file_name().expect("file has no name").to_owned();
            file_name.push(format!("_{}", self.num_written_files));
            self.base_path.with_file_name(&file_name)
        };
        path.set_extension("parquet");
        let file =
            File::create(&path).with_context(|| format!("Could not create {}", path.display()))?;
        let file_writer = ParquetWriter::try_new(file, self.schema.clone(), Some(self.properties.clone()))
            .with_context(|| {
                format!(
                    "Could not create writer for {} with schema {} and properties {:?}",
                    path.display(),
                    self.schema,
                    self.properties.clone()
                )
            })?;

        self.file_writer = Some((path, file_writer));
        Ok(())
    }
    /// Flushes the internal buffer is too large, then returns the array builder.
    pub fn builder(&mut self) -> Result<&mut Builder> {
        if self.builder.len() >= self.autoflush_row_group_len {
            self.flush()?;
        }
        if let Some(autoflush_buffer_size) = self.autoflush_buffer_size {
            if self.builder.buffer_size() >= autoflush_buffer_size {
                self.flush()?;
            }
        }

        Ok(&mut self.builder)
    }
}

impl<Builder: Default + StructArrayBuilder> Drop for ParquetTableWriter<Builder> {
    fn drop(&mut self) {
        if self.file_writer.is_some() {
            self.flush().unwrap();
            let (path, file_writer) = self.file_writer
                .take()
                .unwrap();
            file_writer
                .close()
                .with_context(|| format!("Could not close {}", path.display()))
                .unwrap();
        }
    }
}
