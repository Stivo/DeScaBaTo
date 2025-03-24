package ch.descabato.core.actions

import ch.descabato.Main
import ch.descabato.core.BackupException
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.BackupEnv
import ch.descabato.core.model.ChunkKey
import ch.descabato.core.model.Size
import ch.descabato.core.model.ValueLogStatusKey
import ch.descabato.frontend.Command
import ch.descabato.frontend.VerifyConf
import ch.descabato.protobuf.keys.FileMetadataValue
import ch.descabato.protobuf.keys.ValueLogIndex
import ch.descabato.protobuf.keys.ValueLogStatusValue
import ch.descabato.utils.Implicits.AwareDigest
import ch.descabato.utils.Utils
import org.bouncycastle.crypto.Digest

import java.io.File
import java.io.IOException
import java.io.PrintWriter
import java.nio.file.Files
import scala.util.Random
import scala.util.Try
import scala.util.Using


class VerifyCommand(verifyConf: VerifyConf, backupFolderConf: BackupFolderConfiguration)
  extends Command {

  def run(): Unit = {
    val counter = new DoVerify(backupFolderConf).verifyAll(verifyConf)
    Main.lastErrors = counter.count
    if (counter.count != 0)
      Main.exit(counter.count.toInt)
  }

}

class DoVerify(conf: BackupFolderConfiguration) extends AutoCloseable with Utils {

  private val backupEnv = BackupEnv(conf, readOnly = true)

  import backupEnv.*

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

  // TODO allow usage of this function
  private def checkHashesSimple(counter: ProblemCounter): Unit = {
    val keys = backupEnv.rocks.getAllValueLogStatusKeys().toSeq.sortBy(_._1.name)
    for ((key, value) <- keys) {
      logger.info(s"Hashing $key")
      val path = backupEnv.config.resolveRelativePath(key.name)
      val hash = Using(Files.newInputStream(path.toPath)) { is =>
        org.apache.commons.codec.digest.DigestUtils.md5Hex(is)
      }
      val expectedHash = s"${value.md5Hash.toString().substring(4)}"
      if (hash.isSuccess) {
        if (expectedHash != hash.get) {
          counter.addProblem(s"Type 1: Hash for ${key.name} does not match between expected $expectedHash and actual ${hash.get}")
        }
      } else {
        counter.addProblem(s"Failed hashing ${key.name}")
      }
    }

  }

  private def checkExport(counter: ProblemCounter): Unit = {
    logger.info(s"Found ${backupEnv.rocks.getAllRevisions().size} revisions")
    logger.info(s"Found ${backupEnv.rocks.getAllValueLogStatusKeys().size} value log status keys")
    logger.info(s"Found ${backupEnv.rocks.getAllFileMetadata().size} value log index keys")
    logger.info(s"Found ${backupEnv.rocks.getAllChunks().size} chunks")
    val uncompressedSize = Size(backupEnv.rocks.getAllChunks().map(_._2.lengthUncompressed.toLong).sum)
    val compressedSize = Size(backupEnv.rocks.getAllChunks().map(_._2.lengthCompressed.toLong).sum)
    logger.info(s"Found $uncompressedSize bytes uncompressed file contents (compressed to $compressedSize)")
    var (files, sizes) = (0L, 0L)
    var uniqueFiles = Set.empty[FileMetadataValue]
    for {
      revisionValue <- backupEnv.rocks.getAllRevisions().values
      identifier <- revisionValue.fileIdentifiers
      (_, value) <- backupEnv.rocks.getFileMetadataById(identifier)
    } {
      files += 1
      sizes += value.length
      uniqueFiles += value
    }
    logger.info(s"Can restore $files files in total with total size ${Size(sizes)}")
    logger.info(s"${uniqueFiles.size} different files with total size ${Size(uniqueFiles.map(_.length).sum)}")
  }

  private def checkConnectivity(t: VerifyConf, problemCounter: ProblemCounter): Unit = {
    for ((rev, value) <- backupEnv.rocks.getAllRevisions().toSeq.sortBy(_._1.number)) {
      logger.info(s"Checking ${value.fileIdentifiers.size} files in revision ${rev.number}")
      val files = value.fileIdentifiers.map(x => (x, rocks.getFileMetadataById(x)))
      for ((id, fileOption) <- files) {
        if (fileOption.isEmpty) {
          problemCounter.addProblem("Missing file metadata for id " + id)
        } else {
          val (fileMetadataKey, fileMetadataValue) = fileOption.get
          for (chunkKey <- fileMetadataValue.hashIds) {
            rocks.getChunkById(chunkKey) match {
              case Some((ChunkKey(hash), c: ValueLogIndex)) =>
                if (c.filename.contains("0762") || c.filename.contains("0788")) {
                  if (!checkedAlready.contains(c)) {
                    checkedAlready += c
                    if ((t.checkFirstOfEachVolume() && c.from < 100) || (t.percentOfFilesToCheck() > 0 && t.percentOfFilesToCheck() >= random.nextInt(100))) {
                      try {
                        if (checkedAlready.size % 100 == 0) {
                          logger.info(s"Checking ${c}")
                        }
                        val value = backupEnv.reader.readValue(c)
                        val computedHash = digest.digest(value)
                        println(s"$computedHash vs $hash")
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
                        case e: IllegalStateException =>
                          problemCounter.addProblem("Other issue, might be a whole corrupted volume " + e.getMessage)
                        case e: Exception =>
                          problemCounter.addProblem("Some other issue " + e.getMessage)
                      }
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