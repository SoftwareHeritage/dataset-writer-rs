// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::path::PathBuf;
use std::io::BufWriter;

use anyhow::{Context, Result};

use crate::TableWriter;

pub type PlainTextZstTableWriter<'a> = BufWriter<zstd::stream::AutoFinishEncoder<'a, File>>;

impl TableWriter for PlainTextZstTableWriter<'_> {
    type Schema = ();
    type CloseResult = ();
    type Config = ();

    fn new(mut path: PathBuf, _schema: Self::Schema, _config: ()) -> Result<Self> {
        path.set_extension("txt.zst");
        let file =
            File::create(&path).with_context(|| format!("Could not create {}", path.display()))?;
        let compression_level = 3;
        let zstd_encoder = zstd::stream::write::Encoder::new(file, compression_level)
            .with_context(|| format!("Could not create ZSTD encoder for {}", path.display()))?
            .auto_finish();
        Ok(BufWriter::new(zstd_encoder))
    }

    fn flush(&mut self) -> Result<()> {
        <_ as std::io::Write>::flush(self).context("Could not flush PlainTextZst writer")
    }

    fn close(mut self) -> Result<()> {
        self.flush().context("Could not close PlainTextZst writer")
    }
}

pub type PlainTextTableWriter = BufWriter<File>;

impl TableWriter for PlainTextTableWriter {
    type Schema = ();
    type CloseResult = ();
    type Config = ();

    fn new(mut path: PathBuf, _schema: Self::Schema, _config: ()) -> Result<Self> {
        path.set_extension("txt");
        let file =
            File::create(&path).with_context(|| format!("Could not create {}", path.display()))?;
        Ok(BufWriter::new(file))
    }

    fn flush(&mut self) -> Result<()> {
        <_ as std::io::Write>::flush(self).context("Could not flush PlainText writer")
    }

    fn close(mut self) -> Result<()> {
        self.flush().context("Could not close PlainText writer")
    }
}
