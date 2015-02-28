DeScaBaTo
=========
(View this file online at TODO for prettier formatting)

The Deduplicating Scala Backup Tool. It is inspired by [duplicati](http://www.duplicati.com/). Currently it only supports local backups. This is the first release, and seems to work fine, but no warranty is given. Test it first before deploying it.

As of now, it has these features:
- Backup and restore files and folders, including metadata (lastModified)
- A deduplicating storage mechanism. Parts of a file that are the same are only saved once. 
- Supports compression (gzip or lzma) and encryption (aes or none)
- Command line interface

Compared to duplicati:
- Duplicati is a mature program with a long history and users, DeScaBaTo is very new
- Both do deduplication in the same way
- DeScaBaTo is faster
- By using md5 as default there is less space required for metadata
- The design is simpler (no database to keep synchronized), and should be more robust

### Installation

Extract the zip file. Start the script bin\descabato.bat on windows and bin/descabato on Linux.

### Limitations

General:
- RAM usage may be high. I think at least 300MB for each Terabyte of data
- No scheduling features
- No remote backups
- No compaction implemented. All versions will be kept. This will be changed later.

Linux:
- Not currently officially supported
- Symbolic links will crash the program
- No file metadata except for last modified time and creation time are stored

Mac:
- Completely untested. Use at your own risk.

Encryption:
- Currently the volumes are not fully encrypted. If someone has a copy of a file and your encrypted backup, he can check if your backup includes that file. This will be changed later.
- Due to US export restrictions, the jvm usually ships without support for AES-256. To use AES with a longer keylength than 128, install the jce policy files (google it).

### Usage

To backup a directory, simply go to the bin folder and launch:

    descabato.bat backup [options] backupDestination folderToBackup

For options, execute help backup. Without options, a backup will be created of the given folder in backupDestination. You will need this folder for every command invocation. For your convenience, a script is written in the bin folder which allows you to backup this backup again.
The backupDestination will define where your backup will be saved. This should be on another disk, not the same physical disk.

Once the backup is created, you can browse it or restore files from it, or verify that it is intact.

Absolutely no warranty given. 
