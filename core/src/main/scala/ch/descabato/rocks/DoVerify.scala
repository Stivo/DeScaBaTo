package ch.descabato.rocks

import java.io.IOException

import ch.descabato.core.BackupException
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.frontend.VerifyConf
import ch.descabato.rocks.protobuf.keys.BackedupFileType
import ch.descabato.rocks.protobuf.keys.FileMetadataValue
import ch.descabato.rocks.protobuf.keys.ValueLogIndex
import ch.descabato.utils.Implicits.AwareDigest
import ch.descabato.utils.Utils

import scala.util.Random

class DoVerify(conf: BackupFolderConfiguration) extends AutoCloseable with Utils {

  private val rocksEnv = RocksEnv(conf, readOnly = true)

  import rocksEnv._

  val random = new Random()
  val digest = conf.createMessageDigest()

  var checkedAlready = Set.empty[ValueLogIndex]

  def verifyAll(t: VerifyConf): ProblemCounter = {
    val out = new ProblemCounter()
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
                          out.addProblem(s"Chunk $c for hash ${chunkKey.hash.base64} does not match the computed hash ${computedHash.base64}.\n" +
                            s"This would be needed to reconstruct ${file.path} for example")
                        }
                      } catch {
                        case e: IOException =>
                          out.addProblem(s"Chunk $c for hash ${chunkKey.hash.base64} could not be read correctly (got exception ${e.getMessage})")
                      }
                    } else {
                      try {
                        rocksEnv.reader.assertChunkIsCovered(c)
                      } catch {
                        case e: IOException =>
                          out.addProblem(s"Chunk $c for hash ${chunkKey.hash.base64} is not covered by the file ${c.filename} (got exception ${e.getMessage})")
                        case e: BackupException =>
                          out.addProblem(s"Chunk $c for hash ${chunkKey.hash.base64} is not covered by the file ${c.filename} (got exception ${e.getMessage})")
                      }
                    }
                  }
                case None =>
                  out.addProblem(s"Can not find chunk for hash ${chunkKey.hash.base64}")
              }
            }
          case None =>
            out.addProblem(s"Can not find metadata for $metadata")
        }
      }
    }
    // check consistency:
    //  is the db export up to date?
    //  are all the files that are on the disk marked as written successfully?
    //  Are all the chunks from all files of all revisions in the value logs?
    //  Can all hmacs be verified with the given password?
    if (out.count == 0) {
      logger.info("No problems found.")
    }
    out
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