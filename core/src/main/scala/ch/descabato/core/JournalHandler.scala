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
    val out = randomAccessFile.getChannel().tryLock()
    if (out == null)
      throw new BackupInUseException
    out
  } catch {
    case e: BackupInUseException => throw e
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
          if (parts(2).length() == parts(1).toInt) {
            if (!(files safeContains parts(2))) {
              l.warn("File is in journal, but not on disk "+files(parts(2)))
            } else {
              // TODO some kind of publish/subscriber mechanism should be used instead of this
              val file = new File(config.folder, parts(2))
              val filetype = fileManager.getFileType(file)
              if (!filetype.isTemp(file) && !filetype.redundant) {
                universe.redundancyHandler.createPar2(file)
              }
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
  
  def finishedFile(file: File, filetype: FileType[_]): Boolean = {
    checkLock()
    randomAccessFile.synchronized {
      val name = file.getName()
      randomAccessFile.seek(randomAccessFile.length())
      val line = "Finished " + name.length() + " " + name + "\r\n"
      randomAccessFile.writeBytes(line)
      randomAccessFile.getFD().sync()
    }
    if (!filetype.isTemp(file) && !filetype.redundant) {
      universe.redundancyHandler.createPar2(file)
    }
    true
  }

  def finish(): Boolean = {
//    cleanUnfinishedFiles()
    lock.release()
    randomAccessFile.close()
    true
  }
}