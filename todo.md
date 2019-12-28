#### MVP for backwards compatible rocks backup:
- Ensure that chunking behaves the same
    - implement lower and upper bounds for chunking to prevent too large values => DONE
    - Test that same boundaries are chosen
- ignore file => DONE
- save config.json to database
- manage state of rocksdb in rocksdb
- save md5 of saved file to rocksdb => DONE

##### exporting metadata
crash resilience still needs some work:
- always track file status in db => Done
- no temp prefix => Done
- file status has statuses: Writing and Finished => Done
- numbers for content values need to be correctly initialized => Done
- Implement repair procedure
- Implement periodic export of metadata
- may need to use temp prefix after all:
    - When rocks db folder is lost it is otherwise impossible to know whether file was written finished
    - When file finishes:
        - First set status to finished in rocks, including writing the md5 hash
        - then rename file to not have temp prefix (if crash in between: rocksdb status wins)  

also should:
- import of metadata into rocksdb when rocksdb is deleted
    - with integration test

#### Import for descabato 0.6 backup
- Ensure that metadata is imported
- Ensure that backup can continue from there
- Write integration test
    - Back up from 0.6.0 code
    - Import to new code
    - touch all files that are in the backup
    - backup with new code
    - ensure that new volumes were written

#### Later features:
- Implement deletion with later compaction
    - implement unused deleting value logs => DONE
    - create new value logs from existing ones
    - implement logical deletion over ftp with transaction commit
- reimplement exporting rocksdb to value folder \
    => deletions as special key value pair with a flag for the key  
- add compression with dictionaries

#### Semantics for backing up metadata into a valuelog
Backup consists of:
1. config.json \
    Is responsible for saving fundamental properties of this backup job. hash, compression, folders etc
1. volumes \
    Value logs with data. The volumes save all the file contents
1. metadata \
    The metadata are value logs with all the metadata inside: revisions, hashes, file metadata etc 
1. rocksdb \
    The rocksdb is used mainly as a cache of the metadata. During writing metadata is first written here,
    and once the backup finishes the metadata is backed up as metadata to the value log folder

Semantics:
1. If the rocksdb exists, it is assumed to be the most complete version of metadata
2. If the rocksdb does not exist (because it was not uploaded to the cloud and the backup was lost), the
metadata value logs will be used to reconstruct the rocksdb content
3. During any write operation (backing up / compaction), writes are only done to rocksdb. Once the backup
finishes the newly added metadata will be backed up to the value logs folder as metadata and then uploaded 
to the cloud.
 
##### States
- Consistent
- Writing
- Reconstructing

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