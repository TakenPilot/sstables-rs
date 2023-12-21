# SSTables

Sorted String Tables

This library has no dependencies.

This is a more generic version of SSTables than Cassandra, and will be readable by any CBOR implementation. This makes it ideal for archiving.

Each item is stored as bytes, assuming the person storing the data will know how to interpret the item.

Great for:

- logging
- event streams
- fast lookups in giant files

Note: this library has no dependencies.
