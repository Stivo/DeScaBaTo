package ch.descabato.rocks

import java.util.Date

import ch.descabato.core.commands.RestoredPathLogic
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.FileAttributes
import ch.descabato.core.model.FolderDescription
import ch.descabato.frontend.RestoreConf
import ch.descabato.rocks.protobuf.keys.FileMetadataKey
import ch.descabato.rocks.protobuf.keys.FileMetadataValue
import ch.descabato.rocks.protobuf.keys.FileType
import ch.descabato.rocks.protobuf.keys.RevisionValue

class DoRestore(conf: BackupFolderConfiguration) {

  val rocksEnv = RocksEnv(conf, readOnly = true)

  def restoreFromDate(t: RestoreConf, date: Date): Unit = {
    val revision = rocksEnv.rocks.getAllRevisions().maxBy(_._1.number)
    restoreRevision(t, revision)
  }

  def restoreLatest(t: RestoreConf): Unit = {
    val revision = rocksEnv.rocks.getAllRevisions().maxBy(_._1.number)
    restoreRevision(t, revision)
  }

  def restoreRevision(t: RestoreConf, revision: (Revision, RevisionValue)): Unit = {
    val (files, folders) = revision._2.files
      .flatMap { key =>
        rocksEnv.rocks.readFileMetadata(FileMetadataKeyWrapper(key)).map(x => (key, x))
      }
      .partition(_._1.filetype == FileType.FILE)
    val logic = new RestoredPathLogic(folders.map(x => FolderDescriptionFactory.apply(x._1, x._2)), t)
    for ((key, value) <- folders) {
      restoreFolder(key, value, logic)
    }
  }

  def restoreFolder(key: FileMetadataKey, value: FileMetadataValue, logic: RestoredPathLogic) = {
    val restoredFile = logic.makePath(key.path)
    restoredFile.mkdirs()
    restoreAttributes(value)
  }

  def restoreAttributes(value: FileMetadataValue) = {
    // TODO
  }

}

object FolderDescriptionFactory {
  def apply(key: FileMetadataKey, value: FileMetadataValue) = {
    new FolderDescription(key.path, new FileAttributes())
  }
}