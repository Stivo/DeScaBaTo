DeScaBaTo
=========
(View this file online at https://github.com/Stivo/DeScaBaTo/tree/master/releases/0.3.0 for prettier formatting)

The Deduplicating Scala Backup Tool. It is inspired by [duplicati](http://www.duplicati.com/). Currently it only supports local backups. This is the third release, and seems to work fine, but no warranty is given. Test it first before using it extensively.

As of now, it has these features:

- Backup and restore files and folders, including metadata (lastModified, access rights on linux)
- A deduplicating storage mechanism. Parts of a file that are the same are only saved once. 
- Supports compression (gzip, lzma, bzip2, snappy) and encryption based on TrueVFS
- Command line interface
- Multithreaded backup possible
- Journal for reliable crash recovery (as tested on continous integration)
- Smart choosing of compression algorithm for each file extension

Compared to duplicati:

- Duplicati is a mature program with a long history and users, DeScaBaTo is very new
- Both do deduplication in the same way
- DeScaBaTo is faster, especially when multi-threaded
- By using md5 for hashing as default there is less space required for metadata
- The design is simpler (no database to keep synchronized), and should be more robust. DeScaBaTo only writes to the target folder
- Duplicati has a gui, supports remote backups and more, DeScaBaTo does not yet have those features

### Installation

Extract the zip file. Start the script bin\descabato.bat on windows and bin/descabato on Linux.

### Limitations

General:

- RAM usage may be high. I think at least 300MB for each Terabyte of data backed up. This will be improved in the future.
- No scheduling features
- No remote backups
- No GUI for configuring & running jobs
- No compaction implemented. All versions will be kept. This will be changed later.

Linux:
TODO test on linux

Mac:

- Completely untested. Use at your own risk.

Encryption:

- Encryption is based on zip.raes driver of TrueVFS. It is not certified or a standard, but was written by an IT consultant
- Due to US export restrictions, the jvm usually ships without support for AES-256. To use AES with a longer keylength than 128, install the jce policy files (google it).

### Usage

To backup a directory, simply go to the bin folder and launch:

    descabato.bat backup [options] backupDestination folderToBackup

For options, execute help backup. Without options, a backup will be created of the given folder in backupDestination. You will need this folder for every command invocation. For your convenience, a script is written in the bin folder which allows you to backup this backup again.
The backupDestination will define where your backup will be saved. This should be on another disk, not the same physical disk.

Once the backup is created, you can browse it or restore files from it, or verify that it is intact.

Do not mess with the backup unless you know what you are doing. Especially the journal file and the main backup.json file should not be touched.

Absolutely no warranty given. 
