// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema;
use parquet::arrow::ArrowWriter as ParquetWriter;
use parquet::file::properties::WriterProperties;
use parquet::format::FileMetaData;

use super::{FlushError, NewDatasetError, NoError, StructArrayBuilder, TableWriter};

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
    path: PathBuf,
    /// See [`ParquetTableWriterConfig::autoflush_row_group_len`]
    pub autoflush_row_group_len: usize,
    /// See [`ParquetTableWriterConfig::autoflush_buffer_size`]
    pub autoflush_buffer_size: Option<usize>,
    file_writer: Option<ParquetWriter<File>>, // None only between .close() call and Drop
    builder: Builder,
}

impl<Builder: Default + StructArrayBuilder> TableWriter for ParquetTableWriter<Builder> {
    type Schema = (Arc<Schema>, WriterProperties);
    type CloseResult = FileMetaData;
    type Config = ParquetTableWriterConfig;

    type NewDatasetError = arrow::error::ArrowError;
    type FlushError = NoError;

    fn new(
        mut path: PathBuf,
        (schema, properties): Self::Schema,
        ParquetTableWriterConfig {
            autoflush_row_group_len,
            autoflush_buffer_size,
        }: Self::Config,
    ) -> Result<Self, NewDatasetError<Self::NewDatasetError>> {
        path.set_extension("parquet");
        let file = File::create(&path).into()?;
        let file_writer =
            ParquetWriter::try_new(file, schema.clone(), Some(properties.clone())).into()?;

        Ok(ParquetTableWriter {
            path,
            // See above, we need to make sure the user does not write more than
            // `properties.max_row_group_size()` minus `autoflush_row_group_len` rows between
            // two calls to self.builder() to avoid uneven group sizes. This seems
            // like a safe ratio.
            autoflush_row_group_len: autoflush_row_group_len
                .unwrap_or(properties.max_row_group_size() * 9 / 10),
            autoflush_buffer_size,
            file_writer: Some(file_writer),
            builder: Builder::default(),
        })
    }

    fn flush(&mut self) -> Result<(), FlushError<Self::FlushError>> {
        // Get built array
        let struct_array = self.builder.finish()?;

        let file_writer = self
            .file_writer
            .as_mut()
            .expect("File writer is unexpectedly None");

        // Write it
        file_writer.write(&struct_array.into()).into()?;
        file_writer.flush().into()?;
    }

    fn close(mut self) -> Result<FileMetaData, FlushError<Self::FlushError>> {
        self.flush().into()?;
        self.file_writer
            .take()
            .expect("File writer is unexpectedly None")
            .close()
            .into()
    }
}

impl<Builder: Default + StructArrayBuilder> ParquetTableWriter<Builder> {
    /// Flushes the internal buffer is too large, then returns the array builder.
    pub fn builder(
        &mut self,
    ) -> Result<&mut Builder, FlushError<<Self as TableWriter>::FlushError>> {
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
            self.file_writer
                .take()
                .unwrap()
                .close()
                .with_context(|| format!("Could not close {}", self.path.display()))
                .unwrap();
        }
    }
}
