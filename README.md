DeScaBaTo [![Build Status](https://travis-ci.org/Stivo/DeScaBaTo.png?branch=master)](https://travis-ci.org/Stivo/DeScaBaTo)
=========

#### Warning: I will soon release a new version, with an incompatible file format. Please wait for 0.2.0.

The Deduplicating Scala Backup Tool. It is inspired by [duplicati](http://www.duplicati.com/). Currently it only supports local backups.

As of now, it has these features:
- Backup and restore files and folders, including metadata (lastModified)
- A deduplicating storage mechanism. Parts of a file that are the same are only saved once. 
- Supports compression (gzip or lzma) and encryption (aes or none)
- Command line interface

Compared to duplicati:
- Duplicati is a mature program with a long history and a userbase, DeScaBaTo is very new
- DeScaBaTo is faster
- By using md5 there is less space required for metadata
- The design is simpler (no database to keep synchronized), and should be more robust

Version 0.2.0 will be soon out (a few days). Changes:
- A new backup format. This will allow for stronger encryption, without severely slowing down reconstruction.
- Par2 files for redundancy. DeScaBaTo will automatically notice and recover when backup files were slightly damaged.
- Some linux support (symlinks & posix attributes)
- Better error handling when modifying the backup folder while backing up

I plan to support these features:
- Useable file browser to restore files. Soonish.
- Upload backup to cloud services. A while out.

### Usage

To backup a directory:

    backup [options] backupFolder folderToBackup
    
Once a backup is executed, a batch file will be created to restart this backup. You don't need to set any options, but you can. The options are:

    -b, --block-size  <arg>         (default = 100.0KB) *
    -c, --compression  <arg>                            
    -h, --hash-algorithm  <arg>     (default = md5)     *
    -k, --keylength  <arg>          (default = 128)
    -p, --passphrase  <arg>                             *
        --prefix  <arg>             (default = )        *
    -s, --serializer-type  <arg>
    -v, --volume-size  <arg>        (default = 100.0MB)
        --help                     Show help message

These options apply to backup only. The options with a star in the end can only be defined on the first run, they can not be changed afterwards. All these options will be stored in the backup, except for the passphrase. 

    restore [options] backupFolder 

    -c, --choose-date
    -p, --passphrase  <arg>
        --pattern  <arg>
        --prefix  <arg>                (default = )
        --relative-to-folder  <arg>
        --restore-to-folder  <arg>
    -r  --restore-to-original-path
        --help                        Show help message

Either restore-to-original-path or restore-to-folder has to be set. --choose-date will ask you for the version from which you want to restore from. Pattern is a simple contains pattern, no regex currently.

Browse will launch an interactive browser to look at the files contained in the backup. It is not possible yet to restore files from within the browser, but it will be added.

    browse [options] backupFolder
    
    -p, --passphrase  <arg>
        --prefix  <arg>        (default = )
        --help                Show help message


To use AES with longer keylength than 128, install the jce policy files (google it). MD5 is old and unsafe, but will do fine here and saves space. SHA1, SHA256 are also supported.

Absolutely no warranty given. 

### Compilation
You will need sbt to compile or to set up eclipse. sbt universal:packageBin will create a zip file which an be distributed.
The project is split up into three parts:
- core: The backup logic and command line interface
- browser: A visual browser for the backups
- integrationtest: A test that runs backups and restores and tests them for correctness

### Implementation idea
The current implementation finds all the files in a directory and hashes them block wise. A block contains a fixed number of bytes of a certain file. To describe a file, the metadata and the hash of the needed blocks are used. 
The blocks are then not needed until the data should be restored and can go on a external medium. Blocks are bundled into zip volumes, so that they can easily be uploaded to remote storage (cloud, ftp etc). 
