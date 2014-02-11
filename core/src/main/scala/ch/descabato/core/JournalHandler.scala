package ch.descabato.core

import java.io.RandomAccessFile
import java.io.File
import ch.descabato.utils.Utils
import ch.descabato.utils.Implicits._

// TODO some javadoc here
class SimpleJournalHandler extends JournalHandler with Utils {

  lazy val journalName = config.prefix + "files-journal.txt"

  lazy val randomAccessFile = new RandomAccessFile(new File(config.folder, journalName), "rw")
  lazy val lock = try {
    randomAccessFile.getChannel().tryLock()
  } catch {
    case e: Exception => throw new BackupInUseException().initCause(e)
  }
  def checkLock() {
    if (!lock.isValid()) {
      throw new BackupInUseException
    }
  }
  
  override def setupInternal() {
    cleanUnfinishedFiles()
  }
  
  def cleanUnfinishedFiles(): Boolean = {
    checkLock()
    var files = fileManager.allFiles().filter(_.isFile()).map(_.getName()).toSet
    randomAccessFile.synchronized {
      randomAccessFile.seek(0)
      var line: String = null
      while ({ line = randomAccessFile.readLine(); line != null }) {
        if (line != null) {
          val parts = line.split(" ", 3)
          l.info("Parts are " + parts.mkString(" "))
          if (parts(2).length() == parts(1).toInt) {
            if (!(files safeContains parts(2))) {
              l.warn("File is in journal, but not on disk "+files(parts(2)))
            }
            // Line has been written completely, is valid
            files -= parts(2)
          }
        }
      }
      files.foreach { f =>
        l.debug(s"Deleting file $f because Journal does not mention it")
        new File(config.folder, f).delete()
      }
      true
    }
  }
  
  def finishedFile(name: String): Boolean = {
    checkLock()
    randomAccessFile.synchronized {
      randomAccessFile.seek(randomAccessFile.length())
      randomAccessFile.writeBytes("Finished " + name.length() + " " + name + "\r\n")
      randomAccessFile.getFD().sync()
    }
    true
  }

  def finish(): Boolean = {
    cleanUnfinishedFiles()
    lock.release()
    randomAccessFile.close()
    true
  }
}