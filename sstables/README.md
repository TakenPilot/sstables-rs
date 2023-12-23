# SSTables

Sorted String Tables

This library has no dependencies.

Each item is stored as bytes, assuming the person storing the data will know how to interpret the item. Often, it'll be a string, JSON, Protobufs, or anything else.

Great for:

- logging
- event streams
- fast lookups in giant files
- archiving to S3-like services

Note that this is a more generic version of SSTables than Cassandra, and will be readable by any CBOR implementation. This makes it ideal for long-term data storage and archiving, since the spec is universal and not dependenct on any previously known information. It's also more performant that protobufs.

Also note that this library assumes you're saving raw text or bytes. If you also want to save the items in a generic way, you could use CBOR for that too.

## Writing

When being written, an indexed sstable table records each new entry to two files, a main file and an index file, and they assume that each entry will occur with an incrementing key.

We do this for performance reasons due to the nature of OSes, memory, and storage. Benchmarks show that appending to two files is more performant than holding indices in memory for very large files.

It means that files can be continuously appended to, which is great for event logs or streaming data.

Each entry also starts with its length, which also helps with reading large files or streaming data.

## Reading

The main file can be read in sequence without using the index file.

For searches, the index file contains a series of indices that point to the file position of each entry. Using the keys of these indices, one can perform searches on extremely large files. This is especially useful with S3-like services that allow you to request ranges of bytes.

## Performance
