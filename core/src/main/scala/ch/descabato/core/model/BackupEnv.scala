package ch.descabato.core.model

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.util.FileManager
import ch.descabato.core.util.InMemoryDb
import ch.descabato.core.util.StandardNumberedFileType
import ch.descabato.core.util.ValueLogReader
import ch.descabato.protobuf.keys.Status.FINISHED
import ch.descabato.protobuf.keys.ValueLogStatusValue
import ch.descabato.utils.Utils
import com.typesafe.scalalogging.LazyLogging

import java.awt.Desktop
import java.awt.Desktop.Action
import java.io
import java.io.File

/**
 * This is the rocks env before rocks is created. It is necessary as an intermediate step
 * to allow the repair logic to act before rocks is created.
 *
 * @param config   The configuration
 * @param readOnly Whether this should be read only
 */
class BackupEnvInit(val config: BackupFolderConfiguration,
                    val readOnly: Boolean) extends Utils {
  val fileManager: FileManager = new FileManager(config)
  val backupFolder: File = config.folder

  if (!backupFolder.exists()) {
    logger.info(s"Creating folders for backup at ${backupFolder}")
    backupFolder.mkdir()
  }

}

class BackupEnv(val backupEnvInit: BackupEnvInit,
                val rocks: KeyValueStore) extends LazyLogging with AutoCloseable {

  def config: BackupFolderConfiguration = backupEnvInit.config

  def fileManager: FileManager = backupEnvInit.fileManager

  private var readerInitialized = false
  lazy val reader: ValueLogReader = {
    readerInitialized = true
    new ValueLogReader(this)
  }

  override def close(): Unit = {
    if (readerInitialized) {
      logger.info("Closing reader now")
      reader.close()
    }
  }
}

object BackupEnv extends LazyLogging {
  def apply(config: BackupFolderConfiguration, readOnly: Boolean, ignoreIssues: Boolean = false): BackupEnv = {
    val backupEnvInit = new BackupEnvInit(config, readOnly)
    val kvs = new RepairLogic(backupEnvInit).initialize()
    new BackupEnv(backupEnvInit, kvs)
  }
}


class RepairLogic(backupEnvInit: BackupEnvInit) extends Utils {

  private val fileManager = backupEnvInit.fileManager
  private val dbExportType = fileManager.dbexport
  private val volumeType = fileManager.volume

  private def getFiles(fileType: StandardNumberedFileType): (Seq[io.File], Seq[io.File]) = {
    fileType.getFiles().partition(x => !fileType.isTempFile(x))
  }

  private val (dbExportFiles, dbExportTempFiles) = getFiles(dbExportType)
  private val (volumeFiles, volumeTempFiles) = getFiles(volumeType)

  def deleteFile(x: io.File, readOnly: Boolean): Unit = {
    if (readOnly) {
      logger.info(s"Skipping deletion of file ${x}, because backup is opened as read only")
    } else {
      if (Desktop.isDesktopSupported && Desktop.getDesktop.isSupported(Action.MOVE_TO_TRASH)) {
        Desktop.getDesktop.moveToTrash(x)
      } else {
        x.delete()
      }
    }
  }

  def deleteUnmentionedVolumes(toSeq: Map[ValueLogStatusKey, ValueLogStatusValue], readOnly: Boolean): Unit = {
    var volumes: Set[io.File] = volumeFiles.toSet
    var toDelete = Set.empty[io.File]
    for ((key, path) <- toSeq.toSeq.sortBy(_._1.parseNumber)) {
      val file = backupEnvInit.config.resolveRelativePath(key.name)
      if (path.status == FINISHED) {
        if (volumes.contains(file)) {
          volumes -= file
        }
      } else {
        toDelete += file
        logger.warn(s"Deleting $key, it is not marked as finished in dbexport")
      }
    }
    for (volume <- volumes) {
      logger.warn(s"Will delete $volume, because it is not mentioned in dbexport")
      deleteFile(volume, readOnly)
    }
    for (volume <- toDelete) {
      logger.warn(s"Will delete $volume, because status is not finished")
      deleteFile(volume, readOnly)
    }
  }

  def initialize(): KeyValueStore = {
    if (!backupEnvInit.readOnly) {
      deleteTempFiles()
    }
    val inMemoryDb = InMemoryDb.readFiles(backupEnvInit)
    inMemoryDb.foreach(x => deleteUnmentionedVolumes(x.valueLogStatus, backupEnvInit.readOnly))
    new KeyValueStore(backupEnvInit.readOnly, inMemoryDb.getOrElse(InMemoryDb.empty))
  }

  def deleteTempFiles(): Unit = {
    for (file <- dbExportTempFiles) {
      logger.info(s"Deleting temp file $file")
      file.delete()
    }
    for (file <- volumeTempFiles) {
      logger.info(s"Deleting temp file $file")
      file.delete()
    }
  }

}
