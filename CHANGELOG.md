# v1.2.0

*2025-07-04*

New features:

* Re-export arrow and parquet
* Add support for arrow 54 and 55

Fixes:

* Zstd writer: avoid double dot "..zst" extension

Internal:

* Bump minimal tested version from 1.76 to 1.81 because arrow 55 requires Rust 1.81
* Fixes for Clippy 1.88

# v1.1.0

*2024-10-29*

New feature:

* Add support for UTF8 partitioning columns

Internal:

* Move PartitionedTableWriter to its own module


# v1.0.1

*2024-09-12*

* Add documentation

# v1.0.0

*2024-09-03*

Initial release, imported from [swh-graph](https://archive.softwareheritage.org/swh:1:dir:ea4d3d0db045e123b82fdeefaaa1f76d8ab3f68c;origin=https://gitlab.softwareheritage.org/swh/devel/swh-graph;visit=swh:1:snp:d34d87373bb367ba310002693cb7c4c139c3b882;anchor=swh:1:rev:985dcf705e03fde55285ca8aaff2488f43e9a55f;path=/rust/src/utils/dataset_writer/)
