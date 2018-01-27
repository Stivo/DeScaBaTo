package ch.descabato.core.actors

import java.util.Date

import ch.descabato.core._
import ch.descabato.core.actors.MetadataStorageActor.{AllKnownStoredPartsMemory, BackupDescription, BackupMetaDataStored}
import ch.descabato.core.model._
import ch.descabato.core_old._
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Utils

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class MetadataStorageActor(val context: BackupContext) extends MetadataStorage with JsonUser with Utils {

  val config = context.config

  import context.executionContext

  private var hasChanged = false
  private var hasFinished = false

  private var allKnownStoredPartsMemory = new AllKnownStoredPartsMemory()
  private var thisBackup: BackupDescriptionStored = new BackupDescriptionStored()
  private var notCheckpointed: BackupMetaDataStored = new BackupMetaDataStored()

  private var toBeStored: Set[FileDescription] = Set.empty

  def addDirectory(description: FolderDescription): Future[Boolean] = {
    // TODO check if we have folder with same properties already
    val stored = FolderMetadataStored(BackupIds.nextId(), description)
    notCheckpointed.folders :+= stored
    thisBackup.dirIds += stored.id
    Future.successful(true)
  }

  def startup(): Future[Boolean] = {
    Future {
      val mt = new StandardMeasureTime()
      val metadata = context.fileManagerNew.metadata
      val files = metadata.getFiles().sortBy(x => metadata.numberOfFile(x))
      for (file <- files) {
        readJson[BackupMetaDataStored](file) match {
          case Success(data) =>
            allKnownStoredPartsMemory ++= data
          case Failure(f) =>
            logger.info(s"Found corrupt backup metadata in $file, deleting it. Exception: ${f.getMessage}")
            file.delete()
        }
      }
      logger.info(s"Took ${mt.measuredTime()} to read the metadata")
      true
    }
  }

  def retrieveBackup(date: Option[Date] = None): Future[BackupDescription] = {
    val filesToLoad = date match {
      case Some(d) =>
        context.fileManagerNew.backup.forDate(d)
      case None => context.fileManagerNew.backup.newestFile()
    }
    logger.info(s"Loading backup metadata from $filesToLoad")
    val tryToLoad = readJson[BackupDescriptionStored](filesToLoad).flatMap { bds =>
      Try(allKnownStoredPartsMemory.putTogether(bds))
    }
    Future.fromTry(tryToLoad)
  }

  override def hasAlready(fd: FileDescription): Future[FileAlreadyBackedupResult] = {
    if (toBeStored.safeContains(fd)) {
      Future.successful(Storing)
    } else {
      val metadata = allKnownStoredPartsMemory.mapByPath.get(fd.path)

      val haveAlready = metadata.map(_.checkIfMatch(fd)).getOrElse(false)
      if (haveAlready) {
        Future.successful(FileAlreadyBackedUp(metadata.get.asInstanceOf[FileMetadataStored]))
      } else {
        toBeStored += fd
        hasChanged = true
        Future.successful(FileNotYetBackedUp)
      }
    }
  }

  override def saveFile(fileDescription: FileDescription, hashList: Seq[Long]): Future[Boolean] = {
    // TODO check if we have file already under another id and reuse it if possible
    hasChanged = true
    val id = BackupIds.nextId()
    val metadata = FileMetadataStored(id, fileDescription, hashList)
    allKnownStoredPartsMemory += metadata
    notCheckpointed.files :+= metadata
    toBeStored -= metadata.fd
    thisBackup.fileIds += id
    Future.successful(true)
  }

  override def saveFileSameAsBefore(fileMetadataStored: FileMetadataStored): Future[Boolean] = {
    thisBackup.fileIds += fileMetadataStored.id
    Future.successful(false)
  }

  override def finish(): Future[Boolean] = {
    if (hasChanged) {
      checkpointMetadata()
      val file = context.fileManagerNew.backup.nextFile()
      writeToJson(file, thisBackup)
      hasFinished = true
    }
    Future.successful(true)
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error("Actor was restarted", reason)
  }

  def checkpointMetadata(): Unit = {
    val file = context.fileManagerNew.metadata.nextFile()
    writeToJson(file, notCheckpointed)
    notCheckpointed = new BackupMetaDataStored()
  }

  override def receive(myEvent: MyEvent): Unit = {
    myEvent match {
      case FileFinished(context.fileManagerNew.volume, x, false, _) =>
        logger.info(s"Got volume rolled event to $x")
        if (hasFinished) {
          logger.info("Ignoring as files are already written")
        } else {
          checkpointMetadata()
        }
      case _ =>
      // ignore unknown message
    }
  }
}

object MetadataStorageActor extends Utils {

  class BackupMetaDataStored(var files: Seq[FileMetadataStored] = Seq.empty,
                             var folders: Seq[FolderMetadataStored] = Seq.empty,
                             var symlinks: Seq[SymbolicLink] = Seq.empty
                            ) {
    def merge(other: BackupMetaDataStored): BackupMetaDataStored = {
      logger.info("Merging BackupMetaData")
      new BackupMetaDataStored(files ++ other.files, folders ++ other.folders, symlinks ++ other.symlinks)
    }

  }

  class AllKnownStoredPartsMemory() {
    private var _mapById: Map[Long, StoredPart] = Map.empty
    private var _mapByPath: Map[String, StoredPartWithPath] = Map.empty

    def putTogether(bds: BackupDescriptionStored): BackupDescription = {
      val files = bds.fileIds.map(fileId => _mapById(fileId).asInstanceOf[FileMetadataStored])
      val folders = bds.dirIds.map(dirId => _mapById(dirId).asInstanceOf[FolderMetadataStored].folderDescription)
      BackupDescription(files, folders)
    }

    def +=(storedPart: StoredPart): Unit = {
      _mapById += storedPart.id -> storedPart
      storedPart match {
        case storedPartWithPath: StoredPartWithPath =>
          _mapByPath += storedPartWithPath.path -> storedPartWithPath
        case _ =>
        // nothing further
      }
    }

    def ++=(backupMetaDataStored: BackupMetaDataStored): Unit = {
      var maxId: Long = 0
      for (folder <- backupMetaDataStored.folders) {
        _mapById += folder.id -> folder
        _mapByPath += folder.path -> folder
        maxId = Math.max(maxId, folder.id)
      }
      for (file <- backupMetaDataStored.files) {
        _mapById += file.id -> file
        _mapByPath += file.path -> file
        maxId = Math.max(maxId, file.id)
      }
      BackupIds.maxId(maxId)
    }

    def mapById = _mapById

    def mapByPath = _mapByPath

  }


  // TODO symlinks
  case class BackupDescription(files: Seq[FileMetadataStored], folders: Seq[FolderDescription])

}
