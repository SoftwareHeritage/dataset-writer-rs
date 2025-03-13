// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::TableWriter;

#[derive(Debug, Clone)]
pub struct PlainZstTableWriterConfig {
    pub extension: String,
    pub compression_level: i32,
}

impl Default for PlainZstTableWriterConfig {
    fn default() -> Self {
        PlainZstTableWriterConfig {
            extension: "zst".to_owned(),
            compression_level: 3,
        }
    }
}

pub type PlainZstTableWriter<'a> = zstd::stream::AutoFinishEncoder<'a, File>;

impl<'a> TableWriter for PlainZstTableWriter<'a> {
    type Schema = ();
    type CloseResult = ();
    type Config = PlainZstTableWriterConfig;

    fn new(mut path: PathBuf, _schema: Self::Schema, config: Self::Config) -> Result<Self> {
        path.set_extension(&config.extension);
        let file =
            File::create(&path).with_context(|| format!("Could not create {}", path.display()))?;
        let encoder = zstd::stream::write::Encoder::new(file, config.compression_level)
            .with_context(|| format!("Could not create ZSTD encoder for {}", path.display()))?
            .auto_finish();
        Ok(encoder)
    }

    fn flush(&mut self) -> Result<()> {
        Write::flush(self).context("Could not flush Zst writer")
    }

    fn close(mut self) -> Result<()> {
        Write::flush(&mut self).context("Could not close Zst writer")
    }
}
