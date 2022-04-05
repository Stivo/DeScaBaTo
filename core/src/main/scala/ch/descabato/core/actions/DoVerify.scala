package ch.descabato.core.actions

import ch.descabato.core.BackupException
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.BackupEnv
import ch.descabato.core.model.ChunkKey
import ch.descabato.frontend.VerifyConf
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
    logger.info(s"Found ${backupEnv.rocks.getAllFileMetadata().size} value log index keys")
    logger.info(s"Found ${backupEnv.rocks.getAllChunks().size} chunks")
  }

  private def checkConnectivity(t: VerifyConf, problemCounter: ProblemCounter): Unit = {
    for ((rev, value) <- backupEnv.rocks.getAllRevisions().toSeq.sortBy(_._1.number)) {
      logger.info(s"Checking ${value.fileIdentifiers.size} files in revision ${rev.number}")
      val files = value.fileIdentifiers.map(x => (x, rocks.getFileMetadataByKeyId(x)))
      for ((keyId, fileOption) <- files) {
        if (fileOption.isEmpty) {
          problemCounter.addProblem("Missing file metadata for id " + keyId)
        } else {
          val (fileMetadataKey, fileMetadataValue) = fileOption.get
          for (chunkKey <- fileMetadataValue.hashIds) {
            rocks.getChunkById(chunkKey) match {
              case Some((ChunkKey(hash), c: ValueLogIndex)) =>
                if (!checkedAlready.contains(c)) {
                  checkedAlready += c
                  if ((t.checkFirstOfEachVolume() && c.from < 100) || (t.percentOfFilesToCheck() > 0 && t.percentOfFilesToCheck() >= random.nextInt(100))) {
                    try {
                      logger.info(s"Checking ${c}")
                      val value = backupEnv.reader.readValue(c)
                      val computedHash = digest.digest(value)
                      if (computedHash !== hash) {
                        problemCounter.addProblem(s"Chunk $c for hash ${hash.base64} does not match the computed hash ${computedHash.base64}.\n" +
                          s"This would be needed to reconstruct ${fileMetadataKey.path} for example")
                      }
                    } catch {
                      case e: IOException =>
                        problemCounter.addProblem(s"Chunk $c for hash ${hash.base64} could not be read correctly (got exception ${e.getMessage})")
                    }
                  } else {
                    try {
                      backupEnv.reader.assertChunkIsCovered(c)
                    } catch {
                      case e: IOException =>
                        problemCounter.addProblem(s"Chunk $c for hash ${hash.base64} of file ${fileMetadataKey.path} is not covered by the file ${c.filename} (got exception ${e.getMessage})")
                      case e: BackupException =>
                        problemCounter.addProblem(s"Chunk $c for hash ${hash.base64} of file ${fileMetadataKey.path} is not covered by the file ${c.filename} (got exception ${e.getMessage})")
                    }
                  }
                }
              case None =>
                problemCounter.addProblem("Missing chunk for id " + chunkKey)
            }
          }
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