package ch.descabato.core.actors

import java.io.File
import java.util.Date

import ch.descabato.core._
import ch.descabato.core.actors.MetadataActor.{BackupDescription, BackupMetaDataStored}
import ch.descabato.core.model.{BackupIds, FileMetadata, FolderMetadataStored, HashList}
import ch.descabato.core_old._
import ch.descabato.utils.Implicits._
import ch.descabato.utils.{Hash, Utils}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class MetadataActor(val context: BackupContext) extends BackupFileHandler with JsonUser {
  val logger = LoggerFactory.getLogger(getClass)

  val config = context.config

  import context.executionContext

  private var hasChanged = false
  private var hasFinished = false

  private var previous: Map[String, FileMetadata] = Map.empty
  private var previousDirs: Map[String, FolderMetadataStored] = Map.empty
  private var thisBackupNotCheckpointed: Map[String, FileMetadata] = Map.empty
  private var thisBackupCheckpointed: Map[String, FileMetadata] = Map.empty
  private var thisBackupDirs: Map[String, FolderMetadataStored] = Map.empty

  private var toBeStored: Set[FileDescription] = Set.empty

  def addDirectory(description: FolderDescription): Future[Boolean] = {
    val stored = FolderMetadataStored(BackupIds.nextId(), description)
    thisBackupDirs += description.path -> stored
    Future.successful(true)
  }

  def startup(): Future[Boolean] = {
    Future {
      val files = context.fileManager.backup.getFiles().sortBy(_.getName)
      for (file <- files) {
        loadFile(file) match {
          case Success(data) =>
            previous ++= data.files.map(x => (x.fd.path, x)).toMap
            previousDirs ++= data.folders.map(x => (x.folderDescription.path, x)).toMap
            logger.info(s"Loaded metadata from $file, have currently ${previous.size} files from before")
          case Failure(f) =>
            logger.info(s"Found corrupt backup description in $file, deleting it")
            file.delete()
        }
      }
      val tempFiles = context.fileManager.backup.getTempFiles().sortBy(_.getName)
      for (tempFile <- tempFiles) {
        loadFile(tempFile) match {
          case Success(data) =>
            thisBackupCheckpointed ++= data.files.map(x => (x.fd.path, x)).toMap
            hasChanged = true
            logger.info(s"Found checkpointed data in $tempFile, have currently ${thisBackupCheckpointed.size} files from last run")
          case Failure(f) =>
            logger.info(s"Found corrupt checkpointed data in $tempFile, deleting it")
            tempFile.delete()
        }
      }
      true
    }
  }

  def loadFile(file: File): Try[BackupMetaDataStored] = {
    readJson[BackupMetaDataStored](file)
  }

  def retrieveBackup(date: Option[Date] = None): Future[BackupDescription] = {
    Future {
      val filesToLoad = date match {
        case Some(d) =>
          context.fileManager.getBackupForDate(d)
        case None => context.fileManager.getLastBackup()
      }
      if (filesToLoad.isEmpty) {
        throw new IllegalStateException(s"Did not find any backup files for date $date")
      }
      logger.info(s"Loading backup metadata from $filesToLoad")
      val stored = filesToLoad.map(loadFile).flatMap(_.toOption).reduce(_.merge(_))
      BackupDescription(stored.files, stored.folders.map(_.folderDescription))
    }
  }

  override def hasAlready(fd: FileDescription): Future[FileAlreadyBackedupResult] = {
    if (toBeStored.safeContains(fd)) {
      Future.successful(Storing)
    } else {
      val haveAlready = thisBackupCheckpointed.safeContains(fd.path) ||
        thisBackupNotCheckpointed.safeContains(fd.path) || previous.get(fd.path).map { prev =>
        fd.size == prev.fd.size && !prev.fd.attrs.hasBeenModified(new File(fd.path))
      }.getOrElse(false)
      if (haveAlready) {
        val metadata = previous.get(fd.path).orElse(thisBackupCheckpointed.get(fd.path)).orElse(thisBackupNotCheckpointed.get(fd.path)).get
        Future.successful(FileAlreadyBackedUp(metadata))
      } else {
        toBeStored += fd
        hasChanged = true
        Future.successful(FileNotYetBackedUp)
      }
    }
  }

  override def saveFile(fileDescription: FileDescription, hashList: Hash): Future[Boolean] = {
    hasChanged = true
    val id = BackupIds.nextId()
    val metadata = FileMetadata(id, fileDescription, hashList)
    thisBackupNotCheckpointed += metadata.fd.path -> metadata
    toBeStored -= metadata.fd
    Future.successful(true)
  }

  override def saveFileSameAsBefore(fd: FileDescription): Future[Boolean] = {
    if (previous.safeContains(fd.path)) {
      thisBackupNotCheckpointed += fd.path -> previous(fd.path)
      Future.successful(true)
    } else {
      if (thisBackupCheckpointed.safeContains(fd.path) || thisBackupNotCheckpointed.contains(fd.path)) {
        // nothing to do here
        Future.successful(true)
      } else {
        logger.warn(s"$fd was not part of previous backup, programming error (symlinks)")
        Future.successful(false)
      }
    }
  }

  override def finish(): Future[Boolean] = {
    if (hasChanged) {
      val toSave = thisBackupNotCheckpointed ++ thisBackupCheckpointed
      logger.info(s"Writing metadata of ${toSave.values.size} files")
      val file = context.fileManager.backup.nextFile()
      val metadata = BackupMetaDataStored(toSave.values.toSeq, thisBackupDirs.values.toSeq)
      writeToJson(file, metadata)
      hasFinished = true
      logger.info("Done Writing metadata, deleting temp files")
      context.fileManager.backup.getTempFiles().foreach(_.delete())
    }
    Future.successful(true)
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error("Actor was restarted", reason)
  }

  override def receive(myEvent: MyEvent): Unit = {
    myEvent match {
      case FileFinished(context.fileManager.volume, x, false, _) =>
        logger.info(s"Got volume rolled event to $x")
        if (hasFinished) {
          logger.info("Ignoring as files are already written")
        } else {
          val file = context.fileManager.backup.nextFile(temp = true)
          val metadata = BackupMetaDataStored(thisBackupNotCheckpointed.values.toSeq, thisBackupDirs.values.toSeq)
          thisBackupCheckpointed ++= thisBackupNotCheckpointed
          thisBackupNotCheckpointed = Map.empty
          writeToJson(file, metadata)
        }
      case _ =>
        // ignore unknown message
    }
  }
}

object MetadataActor extends Utils {

  case class BackupMetaDataStored(files: Seq[FileMetadata] = Seq.empty,
                                  folders: Seq[FolderMetadataStored] = Seq.empty,
                                  symlinks: Seq[SymbolicLink] = Seq.empty,
                                  hashLists: Seq[HashList] = Seq.empty
                           ) {
    def merge(other: BackupMetaDataStored): BackupMetaDataStored = {
      logger.info("Merging BackupMetaData")
      BackupMetaDataStored(files ++ other.files, folders ++ other.folders, symlinks ++ other.symlinks, hashLists ++ other.hashLists)
    }

  }

  // TODO symlinks
  case class BackupDescription(files: Seq[FileMetadata], folders: Seq[FolderDescription])

}
