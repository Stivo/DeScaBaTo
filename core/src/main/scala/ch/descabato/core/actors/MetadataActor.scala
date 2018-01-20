package ch.descabato.core.actors

import java.io.File

import ch.descabato.core.actors.MetadataActor.BackupMetaData
import ch.descabato.core.model.FileMetadata
import ch.descabato.core.{BackupFileHandler, JsonUser}
import ch.descabato.core_old._
import ch.descabato.utils.Implicits._
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class MetadataActor(val config: BackupFolderConfiguration) extends BackupFileHandler with JsonUser {
  val logger = LoggerFactory.getLogger(getClass)

  private var hasChanged = false

  private var previous: Map[String, FileMetadata] = Map.empty
  private var thisBackup: Map[String, FileMetadata] = Map.empty
  private var thisBackupDirs: Map[String, FolderDescription] = Map.empty
  private var previousDirs: Map[String, FolderDescription] = Map.empty

  private var toBeStored: Set[FileDescription] = Set.empty

  private val filename = "metadata"
  private val file = new File(config.folder, filename + ".json")

  def addDirectory(description: FolderDescription): Future[Boolean] = {
    thisBackupDirs += description.path -> description
    Future.successful(true)
  }

  def startup(): Future[Boolean] = {
    if (file.exists()) {
      val seq = readJson[BackupMetaData](file)
      previous = seq.files.map(x => (x.fd.path, x)).toMap
      previousDirs = seq.folders.map(x => (x.path, x)).toMap
    }
    Future.successful(true)
  }

  override def backedUpData(): Future[BackupMetaData] = {
    Future.successful(BackupMetaData(previous.values.toSeq, previousDirs.values.toSeq))
  }

  override def hasAlready(fd: FileDescription): Future[Boolean] = {
    val haveAlready = toBeStored.safeContains(fd) || thisBackup.safeContains(fd.path) || previous.get(fd.path).map { x =>
      fd.size == x.fd.size && x.fd.attrs.hasBeenModified(new File(fd.path))
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
      writeToJson(file, metadata)
      logger.info("Done Writing metadata")
    }
    Future.successful(true)
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error("Actor was restarted", reason)
  }
}

object MetadataActor {

  case class BackupMetaData(files: Seq[FileMetadata] = Seq.empty,
                            folders: Seq[FolderDescription] = Seq.empty,
                            symlinks: Seq[SymbolicLink] = Seq.empty,
                            deleted: Seq[FileDeleted] = Seq.empty) {

  }

}