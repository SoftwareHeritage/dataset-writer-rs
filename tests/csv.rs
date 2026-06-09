use std::fs::File;
use std::io::{Read, Write};

use tempfile::TempDir;

use dataset_writer::*;

#[test]
fn test_flush() {
    let tmp_dir = TempDir::new().unwrap();

    let mut dataset_writer =
        ParallelDatasetWriter::<PlainZstTableWriter>::new(tmp_dir.path().to_path_buf())
            .expect("Could not create directory");

    dataset_writer
        .get_thread_writer()
        .unwrap()
        .write_all(b"foo,bar\n")
        .expect("Could not write record");

    dataset_writer.flush().expect("Could not flush");

    for entry in std::fs::read_dir(tmp_dir.path()).expect("Could not read dir") {
        let path = entry.expect("Could not stat entry").path();
        let content =
            zstd::stream::read::Decoder::new(File::open(&path).expect("Could not open entry"))
                .expect("Invalid zstd file")
                .bytes()
                .collect::<Result<Vec<_>, _>>()
                .expect("Could not read");
        assert_eq!(content, b"foo,bar\n");

        let mut reader = csv::ReaderBuilder::new().has_headers(false).from_reader(
            zstd::stream::read::Decoder::new(File::open(path).expect("Could not open entry"))
                .expect("Invalid zstd file"),
        );
        let records: Vec<_> = reader.records().map(|record| record.expect("Could not parse record")).collect();
        assert_eq!(records, vec![csv::StringRecord::from(vec!["foo", "bar"])])
    }
}
