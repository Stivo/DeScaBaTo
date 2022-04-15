DeScaBaTo [![CircleCI](https://circleci.com/gh/Stivo/DeScaBaTo/tree/feature%2Fbetter_serialization.svg?style=svg)](https://circleci.com/gh/Stivo/DeScaBaTo/tree/feature%2Fbetter_serialization)
=========

The Deduplicating Scala Backup Tool. Mainly it is inspired by [duplicati](http://www.duplicati.com/), but the content
defined chunking is inspired by [duplicacy](http://www.duplicacy.com/). Currently it only supports local backups and is
in beta stage. For version 0.8.0 I reimplemented the main backup functionality, now it is single-threaded again and uses
protobuf for serialization. The new implementation is simpler, faster and I have a lot more confidence in it. \

I have been using various versions of the program for years already. My biggest backup for my home folder has:

- been running two years
- 104 revisions
- 400k different chunks of data
- 650k different files backed up with all metadata
- Total of 190mb metadata, compressed to 54 mb

With this it can restore:

- 20 mio files in total (around 20k per revision)
- In total 2.2TB of data (around 20 GB per revision)

Due to the deduplication and the fact that most files don't change, the backup is very size efficient compared to a
naive approach.

There are some theoretical limits with this application, since it keeps all metadata in memory during a run it will run
out of memory if you plan to backup around 10 times more files than I am doing in that version.

As of now, it has these features:

- Backup and restore files and folders [fast](https://github.com/Stivo/DeScaBaTo/wiki/Performance), including metadata (
  lastModifiedTime, posix owner and access rights)
- A deduplicating storage mechanism. Parts of a file that are the same are only saved once. Both over time and over
  different files.
- Supports several compression algorithms and
  chooses [automatically](https://github.com/Stivo/DeScaBaTo/wiki/Smart-Compression-Decider) the best one for each file
  type
- Supports fully encrypted backups (based on
  custom [KvStore archive format](https://github.com/Stivo/DeScaBaTo/wiki/KvStore-archive-format))
- Command line interface
- Supports mounting the backed up data, so it can be browsed with a normal file explorer

Compared to duplicati (this analysis may be outdated):

- Duplicati is a mature program with a long history and a userbase. DeScaBaTo is now 8 years old but has only been used
  by me.
- DeScaBaTo is inspired by Duplicati and has a similar deduplication strategy
- By using a custom archive format there is less space required (Duplicati uses zip files)
- DeScaBaTo can be used as a portable program (no installation required, leaves no traces)
- It can automatically choose for each file extension which compression method to use. Duplicati either disables or
  enables compression based on a static list of extensions.

Compared to duplicacy:

- The restore UI is free for DeScaBaTo, but requires installing of another component (winfsp)
- Duplicacy uses less resources while backing up, due to being written in Go
- Duplicacy supports one remote for multiple different backups, where as here you will have separate backup destinations
  for different backups, they are truly independent
- Duplicacy creates several thousand files, while DeScaBaTo puts all the data in large files, these are easier on the
  filesystem and for uploading to cloud storage

### Installation

1. Install Java 17. I
   use [Amazon Corretto](https://docs.aws.amazon.com/corretto/latest/corretto-17-ug/downloads-list.html).
2. Unpack the release.
3. If you want to use the mount command under windows, you must also install [winfsp](http://www.secfs.net/winfsp/rel/)

### Usage

#### To backup a directory:

    backup [options] backupFolder foldersToBackup

Once a backup is executed, a batch file will be created to restart this backup. You don't need to set any options, but
you can. The options are:

    -c, --compression  <arg>      The compressor to use. Smart chooses best compressor by file extension (default = smart)
    -d, --dont-save-symlinks      Disable backing up symlinks
    -h, --hash-algorithm  <arg>   The hash algorithm to use for deduplication. (default = sha3_256)
    -i, --ignore-file  <arg>      File with ignore patterns
    -k, --keylength  <arg>        Length of the AES encryption key (default = 128)
    -l, --logfile  <arg>          Destination of the logfile of this backup
    -n, --no-script-creation      Disables creating a script to repeat the backup.
    -p, --passphrase  <arg>       The password to use for the backup. If none is supplied, encryption is turned off
    -v, --volume-size  <arg>      Maximum size of the main data files (default = 500 MB)
    --help                        Show help message

trailing arguments: \
backup-destination (required)   Root folder of the backup \
folders-to-backup (required)    Folders to be backed up

Before you start backing up, you can use the count command to get an estimate of how many files and data will be backed
up with the given input folder and the given ignore file. A tool like Treesize Free can help you figure out which files
are important to you and which folders contain too many fast chaning files (caches of browsers etc).

#### To restore data, mounting is the preferred option

Mount will mount the backup as a read-only file system, so it can be browsed with a normal file explorer. Also restoring
from backup can be simply done by copying from there.

    mount [options] --mount-folder Y backupFolder
    
    -l, --logfile  <arg>      Destination of the logfile of this backup
    -p, --passphrase  <arg>   The password to use for the backup. If none is
                            supplied, encryption is turned off
    -h, --help                Show help message

#### Restore without mounting

    restore [options] backupFolder 

    -l, --logfile  <arg>             Destination of the logfile of this backup  
    -p, --passphrase  <arg>          The password to use for the backup. If none is supplied, encryption is turned off      
    --restore-backup  <arg>          Filename of the backup to restore.
    --restore-info  <arg>            Destination of a short summary file of the restore process.
    --restore-to-folder  <arg>       Restore to a given folder
    -r, --restore-to-original-path   Restore files to original path.
    -h, --help                       Show help message

Either restore-to-original-path or restore-to-folder has to be set.

To use encryption with longer keylength than 128, install the jce policy files (google it). SHA3_256 is the default hash
algorithm, but you can also choose Blake2 (160 - 512 bits) or sha3 (256 - 512 bits).

Absolutely no warranty given.

### How to set up automatic weekly backups in windows of files on the system partition

This is a bit more complex, due to windows limitations. To access files on the system partition, a shadow copy is
recommended. I don't think I am allowed to bundle vshadow executable with my program. You should be able to download it
with the
[Windows SDK](https://www.microsoft.com/en-us/download/details.aspx?id=8279) \
See doc/backup-demo.bat for the script I use to do my backups. \
See [Set up scheduled task](https://windowsloop.com/schedule-batch-file-in-task-scheduler/) for a way to run your backup
batch script every week. Be sure to check "run with highest privileges" if you want to create a shadow copy from your
backup script.

### Compilation

You will need sbt to compile or to set up eclipse or intellij idea. sbt pack will create a folder which can be zipped
and distributed. The project is split up into three parts:

- core: The backup logic and command line interface
- fuse: A fuse filesystem implementation so the backup can be browsed as a normal folder
- integrationtest: A test that runs backups, tests the integrity of them and then restores them. Lets the process crash
  while backing up.

### Implementation idea

The current implementation finds all the files in a directory and hashes them block wise. A block contains a variable
number of bytes of a certain file. To describe a file, the metadata and the hash of the needed blocks are used.  
The blocks are bundled into volumes with a configurable maximum size. To add to the backup or browse the contained date,
they are not needed. Only when restoring data the volumes are needed, so they can be safely uploaded to the cloud if
space is needed locally.
