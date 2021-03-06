package ch.descabato.core.actors

import java.util.Date

import ch.descabato.core._
import ch.descabato.core.actors.MetadataStorageActor.{AllKnownStoredPartsMemory, BackupDescription, BackupMetaDataStored}
import ch.descabato.core.commands.ProblemCounter
import ch.descabato.core.model._
import ch.descabato.utils.Implicits._
import ch.descabato.utils.{StandardMeasureTime, Utils}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class MetadataStorageActor(val context: BackupContext, val journalHandler: JournalHandler) extends MetadataStorage with JsonUser with Utils {

  val config = context.config

  import context.executionContext

  private var hasFinished = false

  private var allKnownStoredPartsMemory = new AllKnownStoredPartsMemory()
  private var thisBackup: BackupDescriptionStored = new BackupDescriptionStored()
  private var notCheckpointed: BackupMetaDataStored = new BackupMetaDataStored()

  private var toBeStored: Set[FileDescription] = Set.empty

  def addDirectory(description: FolderDescription): Future[Boolean] = {
    require(!hasFinished)
    allKnownStoredPartsMemory.mapByPath.get(description.path) match {
      case Some(FolderMetadataStored(id, fd)) if fd.attrs == description.attrs =>
        thisBackup.dirIds += id
      case _ =>
        val stored = FolderMetadataStored(BackupIds.nextId(), description)
        notCheckpointed.folders :+= stored
        thisBackup.dirIds += stored.id
    }
    Future.successful(true)
  }

  def startup(): Future[Boolean] = {
    Future {
      val mt = new StandardMeasureTime()
      val metadata = context.fileManager.metadata
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

  def getKnownFiles(): Map[String, FileMetadataStored] = allKnownStoredPartsMemory.mapByPath.collect {
    case (x, f: FileMetadataStored) => (x, f)
  }

  override def verifyMetadataForIdsAvailable(date: Date, counter: ProblemCounter): BlockingOperation = {
    val future = retrieveBackup(Some(date))
    future.value match {
      case Some(Success(backupDescription)) =>
        // verification successful, as otherwise it would throw an exception if some metadata is missing
      case Some(Failure(e)) =>
        counter.addProblem(s"Could not load backup for ${date}")
      case None =>
        throw new IllegalStateException("Implementation was updated")
    }
    new BlockingOperation()
  }

  override def getAllFileChunkIds(): Seq[Long] = {
    allKnownStoredPartsMemory.mapById.collect {
      case (_, f: FileMetadataStored) => f
    }.toSeq.flatMap { f: FileMetadataStored =>
      f.chunkIds.toSeq
    }
  }

  def retrieveBackup(date: Option[Date] = None): Future[BackupDescription] = {
    val filesToLoad = date match {
      case Some(d) =>
        context.fileManager.backup.forDate(d)
      case None =>
        context.fileManager.backup.newestFile().get
    }
    logger.info(s"Loading backup metadata from $filesToLoad")
    val tryToLoad = readJson[BackupDescriptionStored](filesToLoad).flatMap { bds =>
      Try(allKnownStoredPartsMemory.putTogether(bds))
    }
    Future.fromTry(tryToLoad)
  }

  override def hasAlready(fd: FileDescription): Future[FileAlreadyBackedupResult] = {
    require(!hasFinished)
    if (toBeStored.safeContains(fd)) {
      Future.successful(Storing)
    } else {
      val metadata = allKnownStoredPartsMemory.mapByPath.get(fd.path)

      val haveAlready = metadata.map(_.checkIfMatch(fd)).getOrElse(false)
      if (haveAlready) {
        Future.successful(FileAlreadyBackedUp(metadata.get.asInstanceOf[FileMetadataStored]))
      } else {
        toBeStored += fd
        Future.successful(FileNotYetBackedUp)
      }
    }
  }

  override def saveFile(fileDescription: FileDescription, hashList: Seq[Long]): Future[Boolean] = {
    require(!hasFinished)
    // TODO check if we have file already under another id and reuse it if possible
    val id = BackupIds.nextId()
    val metadata = FileMetadataStored(id, fileDescription, hashList.toArray)
    allKnownStoredPartsMemory += metadata
    notCheckpointed.files :+= metadata
    toBeStored -= metadata.fd
    thisBackup.fileIds += id
    Future.successful(true)
  }

  override def saveFileSameAsBefore(fileMetadataStored: FileMetadataStored): Future[Boolean] = {
    require(!hasFinished)
    thisBackup.fileIds += fileMetadataStored.id
    Future.successful(false)
  }

  private def isDifferentFromLastBackup(thisBackup: BackupDescriptionStored) = {
    context.fileManager.backup.newestFile() match {
      case Some(x) =>
        readJson[BackupDescriptionStored](x) match {
          case Success(lastBackup) if lastBackup == thisBackup =>
            false
          case _ => true
        }
      case _ =>
        true
    }
  }

  override def finish(): Future[Boolean] = {
    if (!hasFinished) {
      if (notCheckpointed.files.nonEmpty || notCheckpointed.folders.nonEmpty) {
        writeMetadata(notCheckpointed)
        notCheckpointed = new BackupMetaDataStored()
      }
      if (thisBackup.dirIds.nonEmpty || thisBackup.fileIds.nonEmpty) {
        if (isDifferentFromLastBackup(thisBackup)) {
          // this backup has data and it is different from the last backup
          val file = context.fileManager.backup.nextFile()
          writeToJson(file, thisBackup)
        } else {
          logger.info("Same files as last backup, skipping creation of new file")
        }
      }
      hasFinished = true
    }
    Future.successful(true)
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error("Actor was restarted", reason)
  }

  private def writeMetadata(metadataStored: BackupMetaDataStored): Unit = {
    val file = context.fileManager.metadata.nextFile()
    writeToJson(file, metadataStored)
  }

  private def checkpointMetadata(ids: Set[Long]): Unit = {
    val (allChunksDone, notAllChunksDone) = notCheckpointed.files.partition(_.chunkIds.forall(ids.safeContains))
    val stored = new BackupMetaDataStored(files = allChunksDone, folders = notCheckpointed.folders)
    writeMetadata(stored)
    notCheckpointed = new BackupMetaDataStored(files = notAllChunksDone)
  }

  override def receive(myEvent: MyEvent): Unit = {
    myEvent match {
      case CheckpointedChunks(ids) =>
        if (hasFinished) {
          assert(notCheckpointed.files.isEmpty)
          assert(notCheckpointed.folders.isEmpty)
          logger.info("Ignoring as files are already written")
        } else {
          checkpointMetadata(ids)
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

    override def equals(obj: scala.Any): Boolean = obj match {
      case x: BackupMetaDataStored => files == x.files && folders == x.folders && symlinks == x.symlinks
      case _ => false
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

    // I want to do operator overloading here, scalastyle doesn't agree
    // scalastyle:off
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

    // scalastyle:on

    def mapById = _mapById

    def mapByPath = _mapByPath
  }

  // TODO symlinks
  case class BackupDescription(files: Seq[FileMetadataStored], folders: Seq[FolderDescription])

}
