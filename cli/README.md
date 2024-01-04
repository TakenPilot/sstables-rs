# SSTables CLI

The SSTables CLI is a command line tool for managing large SSTables.

An SSTable is a "Sorted String Table", which is a series of key-value pairs
sorted by key. SSTables are typically stored on disk and are immutable, so
they are often used as a persistent data structure. SSTables
are often used in conjunction with a log-structured merge tree (LSM tree) to
provide efficient reads and writes, as well as efficient compactions.

The SSTables CLI provides a set of commands for managing SSTables. The CLI
supports the following commands:

- `append`: Adds a single key and value to the end of a set of SSTables.
- `merge`: Merges a set of SSTables into a single SSTable, sorted by key, keys missing from index are removed.
- `info`: Prints information about a set of SSTables, such as their size, the
  number of key-value pairs, whether they have an index, the minimum and maximum keys, and
  if they are properly sorted.
- `export`: Exports the key-value pairs in a set of SSTables to a JSON or CSV file.
- `get`: Searches a set of SSTables for a specific key, printing out every occurance. Uses the index file if available.

This particular implementation of SSTables is more general than the SSTables
used in Apache Cassandra and Apache HBase so that it is more useful for long-term
archival and data processing use cases. For example, this implementation's data
file is quite literally only a list of key-value pairs and can be read by any
implementation of CBOR, which is a well-known binary format for JSON. This also
means that compression can be applied directly to the data file, which is not
possible with the SSTables used in Apache Cassandra and Apache HBase due to
how they merged their metedata, indices, bloom filters, and data into a single
file.

Instead, this implementation of SSTables stores its metadata, indices, and bloom
filters in a separate files. This allows the data file to be compressed and
decompressed using common tools without losing access to the metadata, indices,
and bloom filters that are necessary for determining which key-value pairs are
present. This also allows the metadata, indices, and bloom filters to
be compressed and decompressed separately from the data file, which is again
more efficient for most compression algorithms.

Currently, the SSTables library only supports a data file and an index file.

This CLI also supports utilities to operate on a sets of SSTables, useful
for debugging or repairing data from many types of data sources. For example:

- `sort`: Sorts a set of SSTables by key, producing a new SSTable and index that
  are properly sorted by key.
- `keys`: Prints the keys in a set of SSTables.
- `values`: Prints the values in a set of SSTables.
- `index`: Prints the contents of index files, which are keys and file offsets of a set of SSTables.
- `dump`: Dumps the contents of a set of SSTables to stdout or a file.

Example `keys` output:

```
a
b
c
d
e
f
```

Example `values` output:

```
1
2
3
4
5
6
```

Example `dump` command:

```
(0) "a": "1"
(4) "b": "2"
(8) "c": "3"
(12) "d": "4"
(16) "e": "5"
(20) "f": "6"
```

Example `index` command:

```
"a": 0
"b": 4
"c": 8
"d": 12
"e": 16
"f": 20
```

# Command line operations

The input and output of the SSTables CLI can be piped to and from other
utilities, such as `jq`, `sort`, `grep`, `head`, `tail`, `wc`, `uniq`, `cut`,
`awk`, `sed`, `tr`, `xargs`, `parallel`, `paste`, `join`, `comm`, `diff`,
`shuf`, `split`, `tee`, `xargs`, `parallel`, and so on to perform more complex
operations.

For example, to filter the keys in an SSTable with json values that match a given
regular expression, you can use the following command:

```bash
sstable values data.sst | jq -r 'keys[] | select(test("regex"))'
```

To find the values in an SSTable with json values that match a given predicate, you can use the
following command:

```bash
sstable values data.sst | jq -r 'keys[] | select(.predicate)'
```

To filter the values in an SSTable that match a grep pattern, you can use the
following command:

```bash
sstable values data.sst | grep pattern
```

To run a command on each value in an SSTable, you can use the following
command:

```bash
sstable values data.sst | xargs -I{} command {}
```

To reformat or transform the values in an SSTable, you can use the following
command:

```bash
sstable values data.sst | sed 's/old/new/g'
```

To use awk to perform a multi-line map-reduce operation on the values in an
SSTable. For example, to sum a certain number within a value, you can use
the following awk command:

```bash
sstable values data.sst | awk '
  BEGIN {
    sum = 0
  }
  {
    // match the third number in the value
    if ($3 ~ /[0-9]+/) {
      sum += $3
    }
  }
  END {
    print sum
  }
'
```

## Inspecting the raw structure of the files

To inspect the raw structure of an SSTable, you can use any CBOR decoder. You
can also use the `cbor` command line tool, which is available on most Linux
distributions. For example, to inspect the envelope of an SSTable, you can use
the following command:

```bash
cbor dump data.sst
```

This is useful for debugging and understanding how the SSTable is structured, or
for writing your own tools to operate on SSTables. It can also reveal bugs in
the SSTables CLI or the SSTables library, as well as bugs in the CBOR
implementation.

The SSTable library and CLI are designed to be as simple as possible, so they
only implement the bare minimum necessary to read and write SSTables, and it
means that they do not implement the entire CBOR specification.

Every file is assumed to be a series of length-delimited CBOR items as if
it were an indefinite-length map, with the length of the item is encoded
before the item itself. This means that the files can be read and written in a streaming
fashion, and it means that the files can be compressed and decompressed using any compression
algorithm without losing the ability to read the file. This also means that the data
can be read and written in parallel, which is useful for large files. Also,
this means that the files can be read and written using any programming
language that supports CBOR.

There are only two types of files: data files and index files. Data files are a series of
key-value pairs where keys can be a utf8 string, bytes, or a positive number and values are
assumed to be a utf8 string or bytes. We traverse the data file using the index file, which
is a series of key-offset pairs. Index keys can be a utf8 string, bytes, or a positive number
and offsets are assumed to be a positive number. The index file is sorted by key, and the
offsets are assumed to be in ascending order.

The SSTable library and CLI do not require that the keys in the data file are
sorted or unique to operate on them. You can always resort the keys
using the `sort` command after you've collected your data, and the `info`
command will tell you if the keys are sorted or not, and will also inform you if
there are any duplicate keys.

## TODO

- Bloom filters
- Secondary indices
- `split`: Splits a set of SSTables into multiple SSTables.
- `validate`: Validates an SSTable.
- `import`: Imports the key-value pairs in a JSON or CSV file to a set of SSTables.
- `sample`: Samples the key-value pairs in a set of SSTables.
- `head`: Prints the first N key-value pairs in a set of SSTables.
- `tail`: Prints the last N key-value pairs in a set of SSTables.
- `min`: Finds the minimum key in a set of SSTables.
- `max`: Finds the maximum key in a set of SSTables.
- `count`: Counts the number of key-value pairs in a set of SSTables.
- `range`: Prints the key-value pairs in a set of SSTables that are within a
  given range.
- `histogram`: Prints a histogram of the keys in a set of SSTables.
- `diff`: Compares two sets of SSTables, printing the keys that are present in
  one set but not the other
- `intersect`: Intersects two sets of SSTables, printing the keys that are
  present in both sets.
- `union`: Unions two sets of SSTables, printing the keys that are present in
  either set.
- `subtract`: Subtracts two sets of SSTables, printing the keys that are
  present in the first set but not the second set.
- Exporting to Parquet, ORC, Arrow, Avro, and other columnar formats
- Exporting to SQLite, RocksDB, LMDB, and other databases
- Exporting to CSV, JSON, and other formats
- Exporting to Kafka, Kinesis, and other streaming data sources
- Exporting to S3, GCS, and other cloud storage services
- Importing from CSV, JSON, and other formats
- Importing from Kafka, Kinesis, and other streaming data sources
- Importing from S3, GCS, and other cloud storage services
- Importing from HTTP, FTP, and other network protocols
