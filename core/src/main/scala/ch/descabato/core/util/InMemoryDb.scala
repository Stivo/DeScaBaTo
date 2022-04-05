package ch.descabato.core.util

import ch.descabato.core.model.BackupEnv
import ch.descabato.core.model.BackupEnvInit
import ch.descabato.core.model.ChunkMap
import ch.descabato.core.model.FileMetadataMap
import ch.descabato.core.model.RevisionKey
import ch.descabato.core.model.ValueLogStatusKey
import ch.descabato.core.util.Implicits.PimpedTry
import ch.descabato.protobuf.keys.ProtoDb
import ch.descabato.protobuf.keys.RevisionValue
import ch.descabato.protobuf.keys.ValueLogStatusValue

import scala.util.Using

/**
 * Responsible for knowing the current state of the metadata. It will always include everything that was exported in earlier runs,
 * plus anything that was written in this run.
 */
class InMemoryDb private(private var _revisionMap: Map[RevisionKey, RevisionValue] = Map.empty,
                         private var _statusMap: Map[ValueLogStatusKey, ValueLogStatusValue] = Map.empty,
                         private val _chunkMap: ChunkMap = ChunkMap.empty,
                         private val _fileMetadataMap: FileMetadataMap = FileMetadataMap.empty
                        ) {

  // revisions
  def revisions: Map[RevisionKey, RevisionValue] = _revisionMap

  def addRevision(key: RevisionKey, value: RevisionValue): Unit = {
    _revisionMap += (key -> value)
  }

  def getRevision(key: RevisionKey): Option[RevisionValue] = _revisionMap.get(key)

  // status
  def valueLogStatus: Map[ValueLogStatusKey, ValueLogStatusValue] = _statusMap

  def getValueLogStatus(key: ValueLogStatusKey): Option[ValueLogStatusValue] = _statusMap.get(key)

  def addValueLogStatus(key: ValueLogStatusKey, value: ValueLogStatusValue): Unit = {
    _statusMap += (key -> value)
  }

  // chunks and fileMetadata: These would be pure copies of their interface one by one, with no gain in encapsulation?

  def chunkMap: ChunkMap = _chunkMap

  def fileMetadataMap: FileMetadataMap = _fileMetadataMap

  def merge(other: InMemoryDb): InMemoryDb = {
    val newRevisions = _revisionMap ++ other.revisions
    val newStatus = _statusMap ++ other.valueLogStatus
    val newChunkMap = _chunkMap.merge(other.chunkMap)
    val newFileMetadataMap = _fileMetadataMap.merge(other.fileMetadataMap)
    new InMemoryDb(newRevisions, newStatus, newChunkMap, newFileMetadataMap)
  }

}

object InMemoryDb {

  def readFiles(backupEnv: BackupEnvInit): Option[InMemoryDb] = {
    val files = backupEnv.fileManager.dbexport.getFiles()
    var out: Option[InMemoryDb] = None
    for (file <- files) {
      Using(backupEnv.config.newCompressedInputStream(file)) { fis =>
        val db = ProtoDb.parseFrom(fis)
        val inMemoryDb = fromProto(db)
        out = out.map(_.merge(inMemoryDb)) orElse Some(inMemoryDb)
      }.getOrThrow()
    }
    out
  }

  def writeFile(backupEnv: BackupEnv, protoDb: ProtoDb): Unit = {
    // TODO theoretically the updates could be larger than 2GiB
    val newFile = backupEnv.fileManager.dbexport.nextFile(temp = true)
    Using(backupEnv.config.newCompressedOutputStream(newFile)) { fos =>
      protoDb.writeTo(fos)
    }.getOrThrow()
    backupEnv.fileManager.dbexport.renameTempFileToFinal(newFile)
  }

  private def fromProto(protoDb: ProtoDb): InMemoryDb = {
    val chunkMap = ChunkMap.importFromProto(protoDb.chunkMap)
    val fileMetadataMap = FileMetadataMap.importFromProto(protoDb.fileMetadataMap)
    new InMemoryDb(protoDb.revisions, protoDb.status, chunkMap, fileMetadataMap)
  }

  def empty: InMemoryDb = new InMemoryDb()
}