DeScaBaTo
=========

The Deduplicating Scala Backup Tool. It is inspired by duplicati. It could potentially become useful for backing up data locally. I just started writing this tool, so the functionality right now is very basic.

As of now, it has these features:
- Backup and restore files and folders, including metadata (lastModified)
- A deduplicating storage mechanism. Parts of a file that are the same are only saved once.
- Supports compression (none or gzip) and encryption (aes or none)
- Command line interface

### Compilation
Besides the normal dependencies it depends on a modified version of Sumac. Clone my sumac repository here and publish it locally with sbt (core/publishLocal) before compiling DeScaBaTo.
You will need sbt to compile or to set up eclipse. sbt assembly will create a runnable jar with all dependencies included (~12Mb).

### Implementation idea
The current implementation finds all the files in a directory and hashes them block wise. A block contains a fixed number of bytes of a certain file. To describe a file, the metadata and the hash of the needed blocks are used. The blocks are then not needed on a daily basis and can go on a external medium, however they are useless without the small files describing their contents. In the future I might add blocks to a zip volume, so that they can easily be uploaded to remote storage (cloud, ftp etc). 

### Usage

To backup a file:

    backup [options] backupFolder folderToBackup
    
The options are:

    usage: 
    --blockSize                       Size        	    1 MB
    --hashAlgorithm                   HashMethod  	    md5
    --passphrase                      String      	    null
    --algorithm                       String      	    AES
    --keyLength                       int         	    128
    --compression            (-c)     CompressionMode	none

These options apply to backup, restore and find (except for blockSize which is only for backup). Options are optional.

    restore [options] backupFolder restoreDestinationFolder

The options for restore have to be identical to the options used for a backup. Valid compression modes are (none, zip). if the passphrase is null, no encryption will be used. To use AES with longer keylength than 128, install the jce policy files (google it). MD5 is old and unsafe, but will do fine here and saves space. SHA1, SHA256 are also supported.

    find [options] backupFolder pattern

Will list all files containing pattern in their path. No wildcards or regex for now.

backup and restore will summarize their intended actions before starting. Answer yes or 'y'.

Absolutely no warranty given. Don't rely exclusively on this tool for backup. 