
DeScaBaTo [![Build Status](https://travis-ci.org/Stivo/DeScaBaTo.png?branch=master)](https://travis-ci.org/Stivo/DeScaBaTo)
=========


The Deduplicating Scala Backup Tool. Mainly it is inspired by [duplicati](http://www.duplicati.com/), but the content
defined chunking is inspired by [duplicacy](http://www.duplicacy.com/). Currently it only supports local backups and is
in beta stage. For version 0.8.0 I reimplemented the main backup functionality, now single-threaded and uses RocksDB as
a local DB to avoid data loss. The new implementation is simpler, faster and I have a lot more confidence in it.

As of now, it has these features:

- Backup and restore files and folders [fast](https://github.com/Stivo/DeScaBaTo/wiki/Performance), including metadata (lastModifiedTime, posix owner and access rights)
- A deduplicating storage mechanism. Parts of a file that are the same are only saved once.
- Supports several compression algorithms and
  chooses [automatically](https://github.com/Stivo/DeScaBaTo/wiki/Smart-Compression-Decider) the best one for each file
  type
- Supports fully encrypted backups (based on
  custom [KvStore archive format](https://github.com/Stivo/DeScaBaTo/wiki/KvStore-archive-format))
- Command line interface
- Supports mounting the backed up data as a windows folder

Compared to duplicati (this analysis may be outdated):

- Duplicati is a mature program with a long history and a userbase, DeScaBaTo is very new
- DeScaBaTo is inspired by Duplicati and shares the same deduplication strategy and has a similar storage format
- By using a custom archive format there is less space required
- Can be used as a portable program (no installation required, leaves no traces)
- It can automatically choose for each file extension which compression method to use. Duplicati either disables or
  enables compression based on a static list of extensions.

Compared to duplicacy:

- The restore UI is free for DeScaBaTo, but requires installing of another component (winfsp)
- Duplicacy uses less resources while backing up, due to being written in Go
- Duplicacy supports one remote for multiple backups, where as here you have a local backup and a remote one always

I plan to support these features:

- Patterns for backups and restores

### Usage

To backup a directory:

    backup [options] backupFolder folderToBackup
    
Once a backup is executed, a batch file will be created to restart this backup. You don't need to set any options, but you can. The options are:

    -c, --compression  <arg>      The compressor to use. Smart chooses best
                                  compressor by file extension (default = smart)
    -d, --dont-save-symlinks      Disable backing up symlinks
    -h, --hash-algorithm  <arg>   The hash algorithm to use for deduplication.
                                  (default = sha3_256)
    -i, --ignore-file  <arg>      File with ignore patterns
    -k, --keylength  <arg>        Length of the AES encryption key (default = 128)
    -l, --logfile  <arg>          Destination of the logfile of this backup
        --no-gui                   Disables the progress report window
        --no-script-creation      Disables creating a script to repeat the backup.
    -p, --passphrase  <arg>       The password to use for the backup. If none is
                                  supplied, encryption is turned off
    -t, --threads  <arg>          How many threads should be used for the backup
                                  (default = 4)
    -v, --volume-size  <arg>      Maximum size of the main data files
                                  (default = 500.0 MB)
        --help                    Show help message
  
These options apply to backup only. The options with a star in the end can only be defined on the first run, they can not be changed afterwards. To put several backups into the same folder, set the prefix for each backup. All these options will be stored in the backup, except for the passphrase. 

    restore [options] backupFolder 

    -c, --choose-date                Choose the date you want to restore from a
                                     list.
    -l, --logfile  <arg>             Destination of the logfile of this backup
        --no-gui                     Disables the progress report window
    -p, --passphrase  <arg>          The password to use for the backup. If none
                                     is supplied, encryption is turned off
        --restore-backup  <arg>      Filename of the backup to restore.
        --restore-info  <arg>        Destination of a short summary of the restore
                                     process.
        --restore-to-folder  <arg>   Restore to a given folder
    -r, --restore-to-original-path   Restore files to original path.
    -h, --help                       Show help message

Either restore-to-original-path or restore-to-folder has to be set. --choose-date will ask you for the version from which you want to restore from.

Browse will launch an interactive browser to look at the files contained in the backup. It is not possible yet to restore files from within the browser, but it will be added.

    browse [options] backupFolder
    
    -l, --logfile  <arg>      Destination of the logfile of this backup
    -p, --passphrase  <arg>   The password to use for the backup. If none is
                            supplied, encryption is turned off
    -h, --help                Show help message

To use encryption with longer keylength than 128, install the jce policy files (google it). MD5 is old and unsafe, but will do fine here and saves space. SHA1, SHA256 are also supported.

Absolutely no warranty given. 

### Compilation
You will need sbt to compile or to set up eclipse or intellij idea. sbt pack will create a folder which can be zipped and distributed.
The project is split up into three parts:
- core: The backup logic and command line interface
- browser: A visual browser for the backups
- integrationtest: A test that runs backups, tests the integrity of them and then restores them. Lets the process crash while backing up.

### Implementation idea
The current implementation finds all the files in a directory and hashes them block wise. A block contains a fixed number of bytes of a certain file. To describe a file, the metadata and the hash of the needed blocks are used. 
The blocks are then not needed until the data should be restored and can go on a external medium. Blocks are bundled into archives, so that they can easily be uploaded to remote storage (cloud, ftp etc). 
