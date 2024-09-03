// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::path::PathBuf;

use arrow::datatypes::Schema;
use arrow::ipc::writer::FileWriter;

use crate::{FlushError, NewDatasetError, NoError, StructArrayBuilder, TableWriter};

/// Writer to a .arrow file, usable with [`ParallelDatasetWriter`](super::ParallelDatasetWriter)
///
/// `Builder` should follow the pattern documented by
/// [`arrow::builder`](https://docs.rs/arrow/latest/arrow/array/builder/index.html)
pub struct ArrowTableWriter<Builder: Default + StructArrayBuilder> {
    path: PathBuf,
    file_writer: FileWriter<File>,
    builder: Builder,
    pub flush_threshold: usize,
}

impl<Builder: Default + StructArrayBuilder> TableWriter for ArrowTableWriter<Builder> {
    type Schema = Schema;
    type CloseResult = ();
    type Config = Option<usize>;

    type NewDatasetError = arrow::error::ArrowError;
    type FlushError = Builder::FinishError;

    fn new(
        mut path: PathBuf,
        schema: Self::Schema,
        config: Option<usize>,
    ) -> Result<Self, NewDatasetError<Self::NewDatasetError>> {
        path.set_extension("arrow");
        let file = File::create(&path)?;
        let file_writer = FileWriter::try_new(file, &schema).map_err(NewDatasetError::Schema)?;

        Ok(ArrowTableWriter {
            path,
            file_writer,
            flush_threshold: config.unwrap_or(1024 * 1024), // Arbitrary
            builder: Builder::default(),
        })
    }

    fn flush(&mut self) -> Result<(), FlushError<Self::FlushError>> {
        let mut tmp = Builder::default();
        std::mem::swap(&mut tmp, &mut self.builder);
        let struct_array = tmp.finish().map_err(FlushError::BuildArray)?;
        self.file_writer.write(&struct_array.into()).map_err(FlushError::Serialize)?;
        Ok(())
    }

    fn close(mut self) -> Result<(), FlushError<Self::FlushError>> {
        self.flush()?;
        self.file_writer
            .finish()
            .map_err(FlushError::Serialize)
    }
}

impl<Builder: Default + StructArrayBuilder> ArrowTableWriter<Builder> {
    /// Flushes the internal buffer is too large, then returns the array builder.
    pub fn builder(
        &mut self,
    ) -> Result<&mut Builder, FlushError<<Self as TableWriter>::FlushError>> {
        if self.builder.len() >= self.flush_threshold {
            self.flush()?;
        }

        Ok(&mut self.builder)
    }
}

impl<Builder: Default + StructArrayBuilder> Drop for ArrowTableWriter<Builder> {
    fn drop(&mut self) {
        self.flush().unwrap();
        self.file_writer
            .finish()
            .expect(&format!("Could not finish {}", self.path.display()));
    }
}
