// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::TableWriter;

pub type CsvZstTableWriter<'a> = csv::Writer<zstd::stream::AutoFinishEncoder<'a, File>>;

impl<'a> TableWriter for CsvZstTableWriter<'a> {
    type Schema = ();
    type CloseResult = ();
    type Config = ();

    fn new(mut path: PathBuf, _schema: Self::Schema, _config: ()) -> Result<Self> {
        path.set_extension("csv.zst");
        let file =
            File::create(&path).with_context(|| format!("Could not create {}", path.display()))?;
        let compression_level = 3;
        let zstd_encoder = zstd::stream::write::Encoder::new(file, compression_level)
            .with_context(|| format!("Could not create ZSTD encoder for {}", path.display()))?
            .auto_finish();
        Ok(csv::WriterBuilder::new()
            .has_headers(true)
            .terminator(csv::Terminator::CRLF)
            .from_writer(zstd_encoder))
    }

    fn flush(&mut self) -> Result<()> {
        self.flush().context("Could not flush CsvZst writer")
    }

    fn close(mut self) -> Result<()> {
        self.flush().context("Could not close CsvZst writer")
    }
}
