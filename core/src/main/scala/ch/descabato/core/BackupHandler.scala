package ch.descabato.core

import scala.collection.mutable.Buffer
import java.io.File
import ch.descabato.utils.Utils
import java.io.IOException
import java.nio.channels.FileLock
import java.io.RandomAccessFile
import java.io.FileInputStream
import java.io.FileNotFoundException
import ch.descabato.ByteArrayOutputStream
import ch.descabato.utils.Streams._

trait BackupRelatedHandler {
  def config: BackupFolderConfiguration
  var _lock: Option[(RandomAccessFile, FileLock)] = None

  def takeLock() {
    val file = new File(config.folder, config.configFileName)
    try {
      val fis = new RandomAccessFile(file, "rw")
      val lock = fis.getChannel().tryLock()
      _lock = Some((fis, lock))
    } catch {
      case e: IOException => throw new BackupInUseException().initCause(e)
    }
  }

  def releaseLock() {
    _lock match {
      case Some((file, lock)) =>
        lock.close(); file.close()
      case None =>
    }
  }

  def statistics(x: BackupDescription) = {
    val num = x.size
    val totalSize = new Size(x.files.map(_.size).sum)
    var out = f"$num%8d, $totalSize"
    //    if ((1 to 10) contains num) {
    //      out += "\n" + x.map(_.path).mkString("\n")
    //    }
    out
  }

}

class BackupHandler(val universe: Universe) extends Utils with BackupRelatedHandler with BackupProgressReporting {

  def config = universe.config
  
  import universe._

  def backup(files: Seq[File]) {
    config.folder.mkdirs()
    takeLock()

    l.info("Starting backup")
    // Walk tree and compile to do list
    val visitor = new OldIndexVisitor(backupPartHandler.readBackup(None).asMap(), recordNew = true, recordUnchanged = true, progress = Some(scanCounter)).walk(files)
    val (newDesc, unchangedDesc, deletedDesc) = (visitor.newDesc, visitor.unchangedDesc, visitor.deletedDesc)

    l.info("Counting of files done")
    l.info("New Files      : " + statistics(newDesc))
    l.info("Unchanged files: " + statistics(unchangedDesc))
    l.info("Deleted files  : " + statistics(deletedDesc))
    if ((newDesc.size + deletedDesc.size) == 0) {
      l.info("No files have been changed, aborting backup")
      return
    }
    backupPartHandler.setCurrent(newDesc.merge(unchangedDesc))

    // Backup files 
    setMaximums(newDesc)

    val (success, failed) = newDesc.files.partition {
      backupFileDesc(_)
    }
    println("Successfully backed up "+success.size+", failed "+failed.size)
    // finish
    l.info(backupPartHandler.remaining+" remaining")
    while (backupPartHandler.remaining > 0) {
      l.info(s"Waiting for backup to finish, ${backupPartHandler.remaining} left")
      Thread.sleep(1000)
    }
    cpuTaskHandler.finish
    blockHandler.finish
    hashListHandler.finish
    backupPartHandler.finish
    // Clean up, handle failed entries
    l.info("Backup completed")
    releaseLock()
  }

  def backupFileDesc(fileDesc: FileDescription): Boolean = {
    val byteCounterbackup = byteCounter.current

    var fis: FileInputStream = null
    try {
      // This is a new file, so we start hashing its contents, fill those in and return the same instance
      val file = new File(fileDesc.path)
      fis = new FileInputStream(file)
      //      if (config.renameDetection) {
      //        val oldOne = checkForRename(file, fileDesc, fis)
      //        for (old <- oldOne) {
      //          l.info("Detected rename from " + old.path + " to " + fileDesc.path)
      //          deletedCopy -= old
      //          val out = old.copy(path = fileDesc.path)
      //          out.hash = old.hash
      //          return Some(out)
      //        }
      //        fis.getChannel.position(0)
      //      }
      //lazy val compressionDisabled = compressionFor(fileDesc)
      var i = 0
      val blockHasher = new BlockOutputStream(config.blockSize.bytes.toInt, { block: Array[Byte] =>
        universe.cpuTaskHandler.computeHash(block, config.hashAlgorithm, new BlockId(fileDesc, i))
        i += 1
      })
      val hos = new HashingOutputStream(config.hashAlgorithm)
      val sis = new SplitInputStream(fis, blockHasher :: hos :: Nil)
      sis.readComplete
      sis.close()
      universe.backupPartHandler.hashForFile(fileDesc, hos.out.get)
      fileCounter += 1
      updateProgress()
      true
    } catch {
      case io: IOException if (io.getMessage().contains("The process cannot access the file")) =>
        l.warn("Could not backup file " + fileDesc.path + " because it is locked.")
        false
      // TODO linux add case for symlinks
      case io: IOException if (io.getMessage().contains("The system cannot find the")) =>
        l.info(s"File ${fileDesc.path} has been deleted since starting the backup")
        false
      case io: FileNotFoundException if (io.getMessage().contains("Access is denied")) =>
        l.info(s"File ${fileDesc.path} can not be accessed")
        false
      case e: IOException => throw e
    } finally {
      if (fis != null)
        fis.close()
    }
  }

}