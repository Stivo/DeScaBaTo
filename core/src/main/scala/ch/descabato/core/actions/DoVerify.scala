package ch.descabato.core.actions

import ch.descabato.core.BackupException
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.BackupEnv
import ch.descabato.core.model.FileMetadataKeyWrapper
import ch.descabato.frontend.VerifyConf
import ch.descabato.protobuf.keys.BackedupFileType
import ch.descabato.protobuf.keys.FileMetadataValue
import ch.descabato.protobuf.keys.ValueLogIndex
import ch.descabato.utils.Implicits.AwareDigest
import ch.descabato.utils.Utils
import org.bouncycastle.crypto.Digest

import java.io.IOException
import scala.util.Random

class DoVerify(conf: BackupFolderConfiguration) extends AutoCloseable with Utils {

  private val backupEnv = BackupEnv(conf, readOnly = true)

  import backupEnv._

  val random = new Random()
  val digest: Digest = conf.createMessageDigest()

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

  private def checkExport(counter: ProblemCounter): Unit = {
    logger.info(s"Found ${backupEnv.rocks.getAllRevisions().size} revisions")
    logger.info(s"Found ${backupEnv.rocks.getAllValueLogStatusKeys().size} value log status keys")
    logger.info(s"Found ${backupEnv.rocks.getAllFileMetadatas().size} file metadatas")
    logger.info(s"Found ${backupEnv.rocks.getAllChunks().size} chunks")
  }

  private def checkConnectivity(t: VerifyConf, problemCounter: ProblemCounter): Unit = {
    for ((rev, value) <- backupEnv.rocks.getAllRevisions().toSeq.sortBy(_._1.number)) {
      logger.info(s"Checking ${value.files.size} files in revision ${rev.number}")
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
                        val value = backupEnv.reader.readValue(c)
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
                        backupEnv.reader.assertChunkIsCovered(c)
                      } catch {
                        case e: IOException =>
                          problemCounter.addProblem(s"Chunk $c for hash ${chunkKey.hash.base64} of file ${file.path} is not covered by the file ${c.filename} (got exception ${e.getMessage})")
                        case e: BackupException =>
                          problemCounter.addProblem(s"Chunk $c for hash ${chunkKey.hash.base64} of file ${file.path} is not covered by the file ${c.filename} (got exception ${e.getMessage})")
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

  override def close(): Unit = backupEnv.close()
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