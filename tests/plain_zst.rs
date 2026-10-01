use std::path::Path;
use std::fs::File;
use std::io::{Read, Write};

use tempfile::TempDir;

use dataset_writer::*;

fn check_written(path: &Path) {
    for entry in std::fs::read_dir(path).expect("Could not read dir") {
        let path = entry.expect("Could not stat entry").path();
        let content =
            zstd::stream::read::Decoder::new(File::open(path).expect("Could not open entry"))
                .expect("Invalid zstd file")
                .bytes()
                .collect::<Result<Vec<_>, _>>()
                .expect("Could not read");
        assert_eq!(content, b"foo,bar");
    }
}

#[test]
fn test_implicit_flush() {
    let tmp_dir = TempDir::new().unwrap();

    let dataset_writer =
        ParallelDatasetWriter::<PlainZstTableWriter>::new(tmp_dir.path().to_path_buf())
            .expect("Could not create directory");

    dataset_writer
        .get_thread_writer()
        .unwrap()
        .write_all(b"foo,bar")
        .expect("Could not write record");

    drop(dataset_writer); // implicit flush

    check_written(tmp_dir.path());
}

#[test]
fn test_flush() {
    let tmp_dir = TempDir::new().unwrap();

    let mut dataset_writer =
        ParallelDatasetWriter::<PlainZstTableWriter>::new(tmp_dir.path().to_path_buf())
            .expect("Could not create directory");

    dataset_writer
        .get_thread_writer()
        .unwrap()
        .write_all(b"foo,bar")
        .expect("Could not write record");

    dataset_writer.flush().expect("Could not flush");

    check_written(tmp_dir.path());
}

#[test]
fn test_flush_help () {
    let tmp_dir = TempDir::new().unwrap();

    let mut dataset_writer =
        ParallelDatasetWriter::<PlainZstTableWriter>::new(tmp_dir.path().to_path_buf())
            .expect("Could not create directory");

    dataset_writer
        .get_thread_writer()
        .unwrap()
        .write_all(b"foo,bar")
        .expect("Could not write record");

    dataset_writer.flush().expect("Could not flush");

    check_written(tmp_dir.path());

    // prevent early deletion. the file should be written by flush() nonetheless.
    drop(dataset_writer);
}
