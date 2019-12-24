package ch.descabato.rocks

import java.io.File
import java.io.FileOutputStream
import java.util.Date

import better.files._
import ch.descabato.core.commands.RestoredPathLogic
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.FileAttributes
import ch.descabato.core.model.FolderDescription
import ch.descabato.frontend.RestoreConf
import ch.descabato.rocks.protobuf.keys.FileMetadataKey
import ch.descabato.rocks.protobuf.keys.FileMetadataValue
import ch.descabato.rocks.protobuf.keys.FileType
import ch.descabato.rocks.protobuf.keys.RevisionValue

class DoRestore(conf: BackupFolderConfiguration) extends AutoCloseable {

  private val rocksEnv = RocksEnv(conf, readOnly = true)

  import rocksEnv._

  def restoreFromDate(t: RestoreConf, date: Date): Unit = {
    val revision = rocks.getAllRevisions().maxBy(_._1.number)
    restoreRevision(t, revision)
  }

  def restoreLatest(t: RestoreConf): Unit = {
    val revision = rocks.getAllRevisions().maxBy(_._1.number)
    restoreRevision(t, revision)
  }

  def restoreRevision(t: RestoreConf, revision: (Revision, RevisionValue)): Unit = {
    val (files, folders) = revision._2.files
      .flatMap { key =>
        rocks.readFileMetadata(FileMetadataKeyWrapper(key)).map(x => (key, x))
      }
      .partition(_._1.filetype == FileType.FILE)
    val logic = new RestoredPathLogic(folders.map(x => FolderDescriptionFactory.apply(x._1, x._2)), t)
    for ((key, value) <- folders) {
      restoreFolder(key, value, logic)
    }
    for ((key, value) <- files) {
      restoreFile(key, value, logic)
    }
    for ((key, value) <- folders) {
      restoreFolder(key, value, logic)
    }
  }

  def restoreFolder(key: FileMetadataKey, value: FileMetadataValue, logic: RestoredPathLogic): Boolean = {
    val restoredFile = logic.makePath(key.path)
    restoredFile.mkdirs()
    restoreAttributes(restoredFile, key, value)
  }

  def restoreFile(key: FileMetadataKey, value: FileMetadataValue, logic: RestoredPathLogic): Boolean = {
    val destination = logic.makePath(key.path)
    for {
      in <- reader.createInputStream(value).autoClosed
      out <- new FileOutputStream(destination).autoClosed
    } {
      in.pipeTo(out)
    }
    restoreAttributes(destination, key, value)
  }

  def restoreAttributes(file: File, key: FileMetadataKey, value: FileMetadataValue): Boolean = {
    file.setLastModified(key.changed)
  }

  override def close(): Unit = rocksEnv.close()
}

object FolderDescriptionFactory {
  def apply(key: FileMetadataKey, value: FileMetadataValue): FolderDescription = {
    new FolderDescription(key.path, new FileAttributes())
  }
}