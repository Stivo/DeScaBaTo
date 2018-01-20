package ch.descabato.core.actors

import java.io.File
import java.util.Date

import ch.descabato.core.actors.MetadataActor.BackupMetaData
import ch.descabato.core.model.FileMetadata
import ch.descabato.core.{BackupFileHandler, JsonUser}
import ch.descabato.core_old._
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Utils
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class MetadataActor(val context: BackupContext) extends BackupFileHandler with JsonUser {
  val logger = LoggerFactory.getLogger(getClass)

  val config = context.config

  import context.executionContext

  private var hasChanged = false

  private var previous: Map[String, FileMetadata] = Map.empty
  private var previousDirs: Map[String, FolderDescription] = Map.empty
  private var thisBackup: Map[String, FileMetadata] = Map.empty
  private var thisBackupDirs: Map[String, FolderDescription] = Map.empty

  private var toBeStored: Set[FileDescription] = Set.empty

  def addDirectory(description: FolderDescription): Future[Boolean] = {
    thisBackupDirs += description.path -> description
    Future.successful(true)
  }

  def startup(): Future[Boolean] = {
    Future {
      val files = context.fileManager.backup.getFiles().sortBy(_.getName)
      for (file <- files) {
        val data = loadFile(file)
        logger.info(s"Loading metadata from $file, have currently ${previous.size} files from before")
        previous ++= data.files.map(x => (x.fd.path, x)).toMap
        previousDirs ++= data.folders.map(x => (x.path, x)).toMap
      }
      true
    }
  }

  def loadFile(file: File): BackupMetaData = {
    readJson[BackupMetaData](file)
  }

  def retrieveBackup(date: Option[Date] = None): Future[BackupMetaData] = {
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
      filesToLoad.map(loadFile).reduce(_.merge(_))
    }
  }

  override def hasAlready(fd: FileDescription): Future[Boolean] = {
    val haveAlready = toBeStored.safeContains(fd) || thisBackup.safeContains(fd.path) || previous.get(fd.path).map { prev =>
      fd.size == prev.fd.size && !prev.fd.attrs.hasBeenModified(new File(fd.path))
    }.getOrElse(false)
    if (!haveAlready) {
      toBeStored += fd
      hasChanged = true
    }
    Future.successful(haveAlready)
  }

  override def saveFile(fileMetadata: FileMetadata): Future[Boolean] = {
    hasChanged = true
    thisBackup += fileMetadata.fd.path -> fileMetadata
    toBeStored -= fileMetadata.fd
    Future.successful(true)
  }

  override def saveFileSameAsBefore(fd: FileDescription): Future[Boolean] = {
    if (previous.safeContains(fd.path)) {
      logger.info(s"Reusing previously saved result for $fd")
      thisBackup += fd.path -> previous(fd.path)
      Future.successful(true)
    } else {
      logger.warn(s"$fd was not part of previous backup, programming error (symlinks)")
      Future.successful(false)
    }
  }

  override def finish(): Future[Boolean] = {
    if (hasChanged) {
      logger.info(s"Writing metadata of ${thisBackup.values.size} files")
      val metadata = BackupMetaData(thisBackup.values.toSeq, thisBackupDirs.values.toSeq)
      val file = context.fileManager.backup.nextFile()
      writeToJson(file, metadata)
      logger.info("Done Writing metadata")
    }
    Future.successful(true)
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error("Actor was restarted", reason)
  }
}

object MetadataActor extends Utils {

  case class BackupMetaData(files: Seq[FileMetadata] = Seq.empty,
                            folders: Seq[FolderDescription] = Seq.empty,
                            symlinks: Seq[SymbolicLink] = Seq.empty) {
    def merge(other: BackupMetaData): BackupMetaData = {
      logger.info("Merging BackupMetaData")
      new BackupMetaData(files ++ other.files, folders ++ other.folders, symlinks ++ other.symlinks)
    }


  }

}