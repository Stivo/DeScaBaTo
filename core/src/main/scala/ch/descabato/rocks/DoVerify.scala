package ch.descabato.rocks

import ch.descabato.core.BackupException
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.frontend.VerifyConf
import ch.descabato.rocks.protobuf.keys.BackedupFileType
import ch.descabato.rocks.protobuf.keys.FileMetadataValue
import ch.descabato.rocks.protobuf.keys.Status
import ch.descabato.rocks.protobuf.keys.ValueLogIndex
import ch.descabato.utils.Implicits.AwareDigest
import ch.descabato.utils.Utils

import java.io.IOException
import scala.util.Random

class DoVerify(conf: BackupFolderConfiguration) extends AutoCloseable with Utils {

  private val rocksEnv = RocksEnv(conf, readOnly = true)

  import rocksEnv._

  val random = new Random()
  val digest = conf.createMessageDigest()

  var checkedAlready = Set.empty[ValueLogIndex]

  def verifyAll(t: VerifyConf): ProblemCounter = {
    // check consistency:
    //  is the db export up to date?
    //  are all the files that are on the disk marked as written successfully?
    //  Are all the chunks from all files of all revisions in the value logs?
    //  Can all hmacs be verified with the given password?
    val counter = new ProblemCounter()
    checkExport(counter)
    checkConnectivity(t, counter)
    counter
  }

  private def checkExport(counter: ProblemCounter) = {
    val importer = new DbMemoryImporter(rocksEnv.rocksEnvInit, rocks)
    val inMemoryDb = importer.importMetadata()
    checkChunks(counter, inMemoryDb)
    checkFileMetadata(counter, inMemoryDb)
    checkValueLogStatuses(counter, inMemoryDb)
    //    checkRevisions(counter, inMemoryDb)
  }

  private def checkChunks(counter: ProblemCounter, inMemoryDb: InMemoryDb) = {
    val rocksChunks = rocksEnv.rocks.getAllChunks()
    if (inMemoryDb.chunks.size != rocksChunks.size) {
      counter.addProblem(s"DbExport and DB content have differing counts for chunks: DB Export has ${inMemoryDb.chunks.size}, DB has ${rocksChunks.size}.")
    } else {
      logger.info(s"Both in DB and in export we have ${inMemoryDb.chunks.size} chunks.")
    }
  }

  private def checkFileMetadata(counter: ProblemCounter, inMemoryDb: InMemoryDb) = {
    val rocksFileMetadatas = rocksEnv.rocks.getAllFileMetadatas()
    if (inMemoryDb.fileMetadata.size != rocksFileMetadatas.size) {
      counter.addProblem(s"DbExport and DB content have differing counts for file metadata: DB Export has ${inMemoryDb.chunks.size}, DB has ${rocksFileMetadatas.size}.")
    } else {
      logger.info(s"Both in DB and in export we have ${inMemoryDb.fileMetadata.size} file metadata.")
    }
  }

  private def checkRevisions(counter: ProblemCounter, inMemoryDb: InMemoryDb) = {
    val rocksRevisions = rocksEnv.rocks.getAllRevisions()
    if (inMemoryDb.revision.size != rocksRevisions.size) {
      counter.addProblem(s"DbExport and DB content have differing counts for revisions: DB Export has ${inMemoryDb.revision.size}, DB has ${rocksRevisions.size}.")
    } else {
      logger.info(s"Both in DB and in export we have ${inMemoryDb.revision.size} revisions.")
    }
  }

  private def checkValueLogStatuses(counter: ProblemCounter, inMemoryDb: InMemoryDb) = {
    val rocksFileStatusKeys = rocksEnv.rocks.getAllValueLogStatusKeys()
    val inExport = inMemoryDb.valueLogStatus
    if (inExport.size != rocksFileStatusKeys.size && inExport.size + 1 != rocksFileStatusKeys.size) {
      counter.addProblem(s"DbExport and DB content have differing counts for value log statuses: DB Export has ${inExport.size}, DB has ${rocksFileStatusKeys.size}.")
    } else {
      logger.info(s"Both in DB and in export we have ${inExport.size} value log statuses.")
    }
    val maxDbExportNumber = rocksFileStatusKeys.map(_._1).filter(_.name.contains("dbexport")).map(_.parseNumber).max
    for ((dbKey, dbValue) <- rocksFileStatusKeys) {
      if (dbValue.status == Status.FINISHED) {
        inExport.get(dbKey).map { exportValue =>
          if (exportValue != dbValue) {
            counter.addProblem(s"Value for ${dbKey} is different in export and in db: ${exportValue} vs ${dbValue}")
          }
        }.getOrElse {
          if (dbKey.parseNumber == maxDbExportNumber) {
            logger.info(s"Last dbexport $dbKey is not mentioned in the export itself, that is fine")
          } else {
            counter.addProblem(s"Could not find $dbKey in the export")
          }
        }
      } else if (dbValue.status != Status.DELETED) {
        counter.addProblem(s"Got unexpected status ${dbValue.status} for $dbValue")
      }
    }
  }

  private def checkConnectivity(t: VerifyConf, problemCounter: ProblemCounter): Unit = {
    for ((_, value) <- rocksEnv.rocks.getAllRevisions()) {
      for (file <- value.files if file.filetype == BackedupFileType.FILE) {
        val metadata: Option[FileMetadataValue] = rocks.readFileMetadata(FileMetadataKeyWrapper(file))
        metadata match {
          case Some(m) =>
            for (chunkKey <- rocks.getHashes(m)) {
              val chunk = rocks.readChunk(chunkKey)
              chunk match {
                case Some(c) =>
                  if (!checkedAlready.contains(c)) {
                    checkedAlready += c
                    if ((t.checkFirstOfEachVolume() && c.from < 100) || (t.percentOfFilesToCheck() > 0 && t.percentOfFilesToCheck() >= random.nextInt(100))) {
                      try {
                        logger.info(s"Checking ${c}")
                        val value = rocksEnv.reader.readValue(c)
                        val computedHash = digest.digest(value)
                        if (computedHash !== chunkKey.hash) {
                          problemCounter.addProblem(s"Chunk $c for hash ${chunkKey.hash.base64} does not match the computed hash ${computedHash.base64}.\n" +
                            s"This would be needed to reconstruct ${file.path} for example")
                        }
                      } catch {
                        case e: IOException =>
                          problemCounter.addProblem(s"Chunk $c for hash ${chunkKey.hash.base64} could not be read correctly (got exception ${e.getMessage})")
                      }
                    } else {
                      try {
                        rocksEnv.reader.assertChunkIsCovered(c)
                      } catch {
                        case e: IOException =>
                          problemCounter.addProblem(s"Chunk $c for hash ${chunkKey.hash.base64} is not covered by the file ${c.filename} (got exception ${e.getMessage})")
                        case e: BackupException =>
                          problemCounter.addProblem(s"Chunk $c for hash ${chunkKey.hash.base64} is not covered by the file ${c.filename} (got exception ${e.getMessage})")
                      }
                    }
                  }
                case None =>
                  problemCounter.addProblem(s"Can not find chunk for hash ${chunkKey.hash.base64}")
              }
            }
          case None =>
            problemCounter.addProblem(s"Can not find metadata for $metadata")
        }
      }
    }
  }

  override def close(): Unit = rocksEnv.close()
}


class ProblemCounter {

  private var _problems: Seq[String] = Seq.empty

  def addProblem(description: String): Unit = {
    this.synchronized {
      _problems :+= description
      println(s"$description (now at $count problems)")
    }
  }

  def count: Long = _problems.size

  def problems: Seq[String] = _problems
}