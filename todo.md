#### MVP for backwards compatible rocks backup:
- Ensure that chunking behaves the same
    - implement lower and upper bounds for chunking to prevent too large values
    - Test that same boundaries are chosen

#### Import for descabato 0.6 backup
- Ensure that metadata is imported
- Ensure that backup can continue from there


#### Later features:
- Implement deletion with later compaction
    - implement unused deleting value logs => DONE
    - create new value logs from existing ones
    - implement logical deletion over ftp with transaction commit
- reimplement exporting rocksdb to value folder \
    => deletions as special key value pair with a flag for the key  
- add compression with dictionaries



#### DONE:
- implement .. folder change
- Add length of uncompressed block to chunkValue (so skipping can be done without reading many parts of a file)
- Backup folders with their modified data
- Use randomaccessfile in valuereader
- Show reference count in modified date under all files
- add CLI interface
- use logger instead of println
- add encryption
- add integration test