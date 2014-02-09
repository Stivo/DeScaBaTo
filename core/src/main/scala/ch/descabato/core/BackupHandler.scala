package ch.descabato.core

import java.io.File
import ch.descabato.utils.Utils
import java.io.IOException
import java.nio.channels.FileLock
import java.io.RandomAccessFile
import java.io.FileInputStream
import java.io.FileNotFoundException
import ch.descabato.utils.Streams._
import ch.descabato.frontend.CLI
import ch.descabato.frontend.MaxValueCounter
import ch.descabato.frontend.ProgressReporters

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

  var curFileCounter = new MaxValueCounter() {
    val name = "Current file"
    var filename = ""

    override def formatted = filename
  }

  import universe._

  def backup(files: Seq[File]) {
    val started = System.currentTimeMillis()
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

    val (success, failed) = newDesc.files.partition(backupFileDesc)

    println("Successfully backed up " + success.size + ", failed " + failed.size)
    universe.finish()
    // Clean up, handle failed entries
    val time = System.currentTimeMillis()
    l.info("Backup completed in " + format(time - started))
    releaseLock()
  }

  def format(millis: Long) = {
    val seconds = millis / 1000
    val minutes = seconds / 60
    val hours = minutes / 60
    val days = hours / 24
    val add = if (days == 0) "" else days+" days "
    f"$add${hours%24}%02d:${minutes%60}%02d:${seconds%60}%02d"
  }

  def backupFileDesc(fileDesc: FileDescription): Boolean = {
    val byteCounterbackup = byteCounter.current
    curFileCounter.maxValue = fileDesc.size
    curFileCounter.current = 0
    curFileCounter.filename = fileDesc.name
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
      val blockHasher = new BlockOutputStream(config.blockSize.bytes.toInt, {
        block: Array[Byte] =>
          val bid = new BlockId(fileDesc, i)
          universe.cpuTaskHandler.computeHash(block, config.hashAlgorithm, bid)
          universe.hashHandler.hash(bid, block)
          byteCounter += block.length
          curFileCounter += block.length
          updateProgress()
          waitForQueues()
          while (CLI.paused) {
            Thread.sleep(100)
          }
          i += 1
      })
      copy(fis, blockHasher)
      universe.hashHandler.finish(fileDesc)
      fileCounter += 1
      updateProgress()
      true
    } catch {
      case io: IOException if io.getMessage().contains("The process cannot access the file") =>
        l.warn("Could not backup file " + fileDesc.path + " because it is locked.")
        false
      // TODO linux add case for symlinks
      case io: IOException if io.getMessage().contains("The system cannot find the") =>
        l.info(s"File ${fileDesc.path} has been deleted since starting the backup")
        false
      case io: FileNotFoundException if io.getMessage().contains("Access is denied") =>
        l.info(s"File ${fileDesc.path} can not be accessed")
        false
      case e: IOException => throw e
    } finally {
      if (fis != null)
        fis.close()
    }
  }

  override def updateProgress() {
    ProgressReporters.updateWithCounters(fileCounter :: byteCounter :: curFileCounter :: Nil)
  }

}