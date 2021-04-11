Transactions:

- When backup finishes, all updates will be written to a new dbexport file
  - that dbexport file will be renamed from temp_ to final name
  - it mentions all volumes that are part of the backup
- when writing: add all updates to an intermediate place

Classes:

- InMemoryDb: Replaces RocksDB memory storage
- Importer: Reads the import into inmemorydb at startup
- Exporter: Writes a new dbexports file based on update entries
- KeyValueStore: Coordinates these three classes and implements transactions

Startup:

- Delete db exports with temp prefix
- Read all db exports that do not have temp_ prefix
- Delete all unmentioned volumes

Pros / Cons:

- Pro: Much easier, no reliance on rocksdb which seems to be hard to tune and bring memory issues
- Pro: Binary size is decreasing
- Con: No intermediate checkpointing. Either evertyhing is backed up or nothing
