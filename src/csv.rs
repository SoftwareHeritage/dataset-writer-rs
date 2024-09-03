// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::path::PathBuf;

use crate::{FlushError, NewDatasetError, NoError, TableWriter};

pub type CsvZstTableWriter<'a> = csv::Writer<zstd::stream::AutoFinishEncoder<'a, File>>;

impl<'a> TableWriter for CsvZstTableWriter<'a> {
    type Schema = ();
    type CloseResult = ();
    type Config = ();

    type NewDatasetError = NoError;
    type FlushError = NoError;

    fn new(
        mut path: PathBuf,
        _schema: Self::Schema,
        _config: (),
    ) -> Result<Self, NewDatasetError<Self::NewDatasetError>> {
        path.set_extension("csv.zst");
        let file: File = File::create(&path)?;
        let compression_level = 3;
        let zstd_encoder = zstd::stream::write::Encoder::new(file, compression_level)?
            .auto_finish();
        Ok(csv::WriterBuilder::new()
            .has_headers(true)
            .terminator(csv::Terminator::CRLF)
            .from_writer(zstd_encoder))
    }

    fn flush(&mut self) -> Result<(), FlushError<Self::FlushError>> {
        Ok(self.flush()?)
    }

    fn close(mut self) -> Result<(), FlushError<Self::FlushError>> {
        Ok(self.flush()?)
    }
}
