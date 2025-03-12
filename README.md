# dataset-writer-rs

Utilities to write CSV/Arrow/Parquet files concurrently

## CSV example

```
use tempfile::TempDir;
use rayon::prelude::*;

use dataset_writer::*;

let tmp_dir = TempDir::new().unwrap();

let mut dataset_writer = ParallelDatasetWriter::<CsvZstTableWriter>::new(tmp_dir.path().join("dataset"))
    .expect("Could not create directory");

(0..100000)
    .into_par_iter()
    .try_for_each_init(
        || dataset_writer.get_thread_writer().unwrap(),
        |table_writer, number| -> Result<(), csv::Error> {
            table_writer.write_record(&[number.to_string()])
        }
    )
    .expect("Failed to write table");
```

## Plain text example

```
use std::io::Write;

use tempfile::TempDir;
use rayon::prelude::*;

use dataset_writer::*;

let tmp_dir = TempDir::new().unwrap();

let mut dataset_writer = ParallelDatasetWriter::<PlainZstTableWriter>::new(tmp_dir.path().join("dataset"))
    .expect("Could not create directory");

(0..100000)
    .into_par_iter()
    .try_for_each_init(
        || dataset_writer.get_thread_writer().unwrap(),
        |table_writer, number| -> Result<(), std::io::Error> {
            table_writer.write_all(format!("{}\n", number).as_bytes())
        }
    )
    .expect("Failed to write table");
```

## Parquet example

```
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{Array, ArrayBuilder, StructArray, UInt64Builder};
use arrow::datatypes::{Field, Schema};
use arrow::datatypes::DataType::UInt64;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;
use rayon::prelude::*;

use dataset_writer::*;

let tmp_dir = TempDir::new().unwrap();

fn schema() -> Schema {
    Schema::new(vec![Field::new("id", UInt64, false)])
}
let writer_properties = WriterProperties::builder().build();

#[derive(Debug)]
pub struct Builder(UInt64Builder);

impl Default for Builder {
    fn default() -> Self {
        Builder(UInt64Builder::new_from_buffer(
            Default::default(),
            None, // Values are not nullable -> validity buffer not needed
        ))
    }
}

impl StructArrayBuilder for Builder {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn buffer_size(&self) -> usize {
        // No validity slice
        self.len() * 8
    }

    fn finish(&mut self) -> Result<StructArray> {
        let columns: Vec<Arc<dyn Array>> = vec![Arc::new(self.0.finish())];

        Ok(StructArray::new(
            schema().fields().clone(),
            columns,
            None, // nulls
        ))
    }
}

let mut dataset_writer = ParallelDatasetWriter::<ParquetTableWriter<Builder>>::with_schema(
    tmp_dir.path().join("dataset"),
    (Arc::new(schema()), writer_properties)
)
.expect("Could not create directory");

(0..100000)
    .into_par_iter()
    .try_for_each_init(
        || dataset_writer.get_thread_writer().unwrap(),
        |table_writer, number| -> Result<()> {
            table_writer.builder()?.0.append_value(number);
            Ok(())
        }
    )
    .expect("Failed to write table");
```
