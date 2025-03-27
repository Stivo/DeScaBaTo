package ch.descabato.core.actions

import ch.descabato.HashesParser
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.BackupEnv
import ch.descabato.core.model.ChunkKey
import ch.descabato.frontend.CheckFilesConf
import ch.descabato.frontend.Command
import ch.descabato.frontend.FileCounter
import ch.descabato.frontend.ProgressReporters
import ch.descabato.frontend.SizeStandardCounterWithEta
import ch.descabato.frontend.StandardCounter
import ch.descabato.frontend.StandardMaxValueCounter
import ch.descabato.protobuf.keys.BackedupFileType
import ch.descabato.protobuf.keys.FileMetadataValue
import ch.descabato.protobuf.keys.ValueLogIndex
import ch.descabato.remote.RemoteOptions
import ch.descabato.utils.Implicits.AwareDigest
import ch.descabato.utils.Streams.VariableBlockOutputStream
import ch.descabato.utils.Utils
import org.bouncycastle.crypto.Digest
import org.bouncycastle.crypto.digests.MD5Digest

import java.io.File
import java.io.FileInputStream
import java.io.PrintWriter
import scala.util.Failure
import scala.util.Random
import scala.util.Success
import scala.util.Using


class CheckFilesCommand(checkFilesConf: CheckFilesConf, backupFolderConf: BackupFolderConfiguration)
  extends Command {

  def run(): Unit = {
    val counter = new DoCheckFiles(backupFolderConf).checkFiles(checkFilesConf)
  }

}

class DoCheckFiles(conf: BackupFolderConfiguration) extends AutoCloseable with Utils {

  private val backupEnv = BackupEnv(conf, readOnly = true)

  import backupEnv.*

  val random = new Random()
  val digest: Digest = conf.createMessageDigest()
  val digestFullMd5: Digest = new MD5Digest()
  val digestFull: Digest = conf.createMessageDigest()

  var checkedAlready = Set.empty[ValueLogIndex]

  private val buffer = Array.ofDim[Byte](1024 * 1024)

  private val bytesCounter = new SizeStandardCounterWithEta(s"Size")
  private val filesCounter = new StandardMaxValueCounter("Files", 0)
  private val fileProgressCounter = new FileCounter()
  private val brokenFilesCounter = new StandardCounter("Broken Files")

  ProgressReporters.addCounter(filesCounter, bytesCounter, fileProgressCounter, brokenFilesCounter)

  def checkFiles(t: CheckFilesConf): Unit = {
    val alreadySeen = HashesParser.parseFiles
    ProgressReporters.openGui("Backup", new RemoteOptions())
    var brokenFiles = 0
    Using(new PrintWriter("hashes.jsonl")) { hashWriter =>
      val lastRevision = rocks.getAllRevisions().keys.toSeq.maxBy(_.number)
      println(lastRevision)
      val revision = rocks.getAllRevisions().get(lastRevision)
      val filesToCheck = revision.get.fileIdentifiers
      val allFiles = filesToCheck.flatMap { file =>
        rocks.getFileMetadataById(file)
      }.filter(_._1.filetype == BackedupFileType.FILE)
      filesCounter.maxValue += allFiles.length
      bytesCounter.maxValue = allFiles.map(_._2.length).sum

      for {
        (key, value) <- allFiles
      } {
        if (alreadySeen.contains(key.path)) {
          filesCounter += 1
          bytesCounter += value.length
        } else {
          if (!new File(key.path).exists()) {
            logger.warn(s"File ${key.path} is missing on disk")
            filesCounter += 1
            bytesCounter += value.length
          } else {
            digestFull.reset()
            digestFullMd5.reset()
            val result = Using.Manager { use =>
              fileProgressCounter.current = 0
              fileProgressCounter.resetSnapshots()
              fileProgressCounter.fileName = key.path
              fileProgressCounter.maxValue = value.length
              val fis = use(new FileInputStream(key.path))
              val hashes = rocks.getHashes(value)
              use(new VariableBlockOutputStream({ wrapper =>
                if (!hashes.hasNext) {
                  throw new IllegalArgumentException(s"File should have ended already")
                }
                val expectedHash = hashes.next
                digest.reset()
                val hash = digest.digest(wrapper)
                if (expectedHash !== hash) {
                  throw new IllegalArgumentException(s"Hash ${hash} does not match expected hash $expectedHash")
                }
                digestFull.update(wrapper)
                digestFullMd5.update(wrapper)
                val chunkKey = ChunkKey(hash)
                val existing = rocks.getChunk(chunkKey)
                if (existing.isEmpty) {
                  logger.error(s"Couldn't find chunk ${hash} in the backup")
                }
                hashWriter.write(s"""{ "hash": "${hash}", "length": ${wrapper.length} }\n""")
                fileProgressCounter += wrapper.length
              }) {
                chunker =>

                import better.files.*

                fis.pipeTo(chunker, buffer)
              })
              val hash1 = digestFull.digest()
              val hashmd5 = digestFullMd5.digest()
              hashWriter.write(s"""{ "file": "${key.path.replace("\\", "\\\\")}", "hash": "${hash1}", "md5": "${hashmd5}" }\n""")
            }

            result match {
              case Failure(e) =>
                logger.error(s"File ${key.path} seems corrupt ${e.getMessage}")
                brokenFilesCounter += 1
                brokenFiles += 1
              case Success(_) =>
            }
            filesCounter += 1
            bytesCounter += value.length
          }
        }
      }
    }
    println(s"${brokenFiles} Files seem broken.")
  }


  override def close(): Unit = backupEnv.close()
}

