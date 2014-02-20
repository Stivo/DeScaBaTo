DeScaBaTo [![Build Status](https://travis-ci.org/Stivo/DeScaBaTo.png?branch=master)](https://travis-ci.org/Stivo/DeScaBaTo)
=========

### Warning
Version 0.2.0 has shown a problem once recovering from a system crash. Version 0.3.0 will fix this with a journal.
It will be optionally multi-threaded (based on akka), will feature a GUI to show backup progress and will also automatically choose the best compression algorithm to use for each file type. Warning: The file format will also change again.

The Deduplicating Scala Backup Tool. It is inspired by [duplicati](http://www.duplicati.com/). Currently it only supports local backups and is in alpha stage, the program's internals are still heavily changing as I am figuring stuff out.

As of now, it has these features:

- Backup and restore files and folders, including metadata (lastModifiedTime, posix owner and access rights)
- A deduplicating storage mechanism. Parts of a file that are the same are only saved once. 
- Supports compression (gzip, bzip2 or lzma) and encryption (based on [TrueVFS](https://truezip.java.net/truezip-driver/truezip-driver-tzp/))
- Command line interface
- Parity creation: Parity files are created so that broken backup files can be recovered.

Compared to duplicati:

- Duplicati is a mature program with a long history and a userbase, DeScaBaTo is very new
- DeScaBaTo is faster
- By using md5 (other hashes supported as well) there is less space required for metadata
- The design is simpler (no database to keep synchronized), and should be more robust

I plan to support these features:

- Patterns for backups and restores
- Useable file browser to restore files
- Upload backup to remote hosts (cloud storage, ftp etc)

### Usage

To backup a directory:

    backup [options] backupFolder folderToBackup
    
Once a backup is executed, a batch file will be created to restart this backup. You don't need to set any options, but you can. The options are:

    -b, --block-size  <arg>         (default = 100.0KB)  *
    -c, --compression  <arg>        (default = smart)
        --create-indexes
    -d, --dont-save-symlinks
    -h, --hash-algorithm  <arg>     (default = md5)      *
    -k, --keylength  <arg>          (default = 128)
    -l, --logfile  <arg>
    -g, --no-gui
        --no-script-creation
    -n, --noansi
    -p, --passphrase  <arg>                              *
        --prefix  <arg>             (default = )         *
    -s, --serializer-type  <arg>
    -t, --threads  <arg>            (default = 2)
    -v, --volume-size  <arg>        (default = 100.0MB)
        --help                     Show help message

These options apply to backup only. The options with a star in the end can only be defined on the first run, they can not be changed afterwards. To put several backups into the same folder, set the prefix for each backup. All these options will be stored in the backup, except for the passphrase. 

    restore [options] backupFolder 

    -c, --choose-date
    -l, --logfile  <arg>
    -g, --no-gui
    -n, --noansi
    -p, --passphrase  <arg>
        --prefix  <arg>               (default = )
        --restore-to-folder  <arg>
    -r, --restore-to-original-path
        --help                       Show help message

Either restore-to-original-path or restore-to-folder has to be set. --choose-date will ask you for the version from which you want to restore from.

Browse will launch an interactive browser to look at the files contained in the backup. It is not possible yet to restore files from within the browser, but it will be added.

    browse [options] backupFolder
    
    -p, --passphrase  <arg>
        --prefix  <arg>        (default = )
        --help                Show help message

To use encryption with longer keylength than 128, install the jce policy files (google it). MD5 is old and unsafe, but will do fine here and saves space. SHA1, SHA256 are also supported.

Absolutely no warranty given. 

### Compilation
You will need sbt to compile or to set up eclipse. sbt universal:packageBin will create a zip file which an be distributed.
The project is split up into three parts:
- core: The backup logic and command line interface
- browser: A visual browser for the backups
- integrationtest: A test that runs backups, tests the integrity and then restores them. Lets the process crash while backing up.

### Implementation idea
The current implementation finds all the files in a directory and hashes them block wise. A block contains a fixed number of bytes of a certain file. To describe a file, the metadata and the hash of the needed blocks are used. 
The blocks are then not needed until the data should be restored and can go on a external medium. Blocks are bundled into zip volumes, so that they can easily be uploaded to remote storage (cloud, ftp etc). 
