package ch.descabato.core

import java.io.{File, RandomAccessFile}

import ch.descabato.core.kvstore.{KvStoreWriter, KvStoreReaderImpl}
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Utils

import scala.collection.immutable.HashSet

class BlockingOperation

// TODO some javadoc here
class SimpleJournalHandler extends JournalHandler with Utils {

  private var _incosistentBackup = false

  val journalInZipFile = "journalUpdates.txt"
  val updateMarker = "_withUpdate"

  lazy val journalName = config.prefix + "files-journal.txt"

  private var _usedIdentifiers = HashSet[String]()

  var _open = true

  lazy val randomAccessFile = new RandomAccessFile(new File(config.folder, journalName), "rw")
  lazy val lock = try {
    val out = randomAccessFile.getChannel().tryLock()
    if (out == null)
      throw new BackupInUseException()
    out
  } catch {
    case e: BackupInUseException => throw e
    case e: Exception => throw new BackupInUseException().initCause(e)
  }
  def checkLock() {
    if (_open && !lock.isValid()) {
      throw new BackupInUseException
    }
  }
  
  override def setupInternal() {
    cleanUnfinishedFiles()
  }

  def cleanUnfinishedFiles(): BlockingOperation = {
    checkLock()
    var files = fileManager.allFiles().filter(_.isFile()).map(_.getName()).toSet
    randomAccessFile.synchronized {
      randomAccessFile.seek(0)
      var line: String = null
      while ({ line = randomAccessFile.readLine(); line != null }) {
        if (line != null) {
          val parts = line.split(" ", 3)
          if (parts(2).length() == parts(1).toInt) {
            _usedIdentifiers += parts(2)
            if (!(files safeContains parts(2))) {
              if (!(parts(2) contains "temp."))
            	  l.warn("File is in journal, but not on disk "+parts(2))
            } else {
              val file = new File(config.folder, parts(2))
              // Read the journal update. A file within a zip file that tells which files should be deleted
              if (file.exists() && parts(0).endsWith(updateMarker)) {
                // TODO updatemarker
//                val reader = ZipFileHandlerFactory.reader(file, config)
//                val s = new String(reader.getStream(journalInZipFile).readFully(), "UTF-8")
//                _usedIdentifiers ++= s.lines
//                s.lines.foreach { f =>
//                  new File(config.folder, f).delete()
//                }
              }
              // TODO some kind of publish/subscriber mechanism should be used instead of this
//              val filetype = fileManager.getFileType(file)
//              if (!filetype.isTemp(file) && !filetype.redundant) {
//                universe.redundancyHandler.createPar2(file)
//              }
            }
            // Line has been written completely, is valid
            files -= parts(2)
          }
        }
      }
      files.foreach { f =>
        l.debug(s"Checking file $f because Journal does not mention it")
        _incosistentBackup = true
        val file = new File(config.folder, f)
        var shouldDelete = true
        if (file.getName.endsWith(".zip.raes") || file.getName.endsWith(".zip")) {
          val kvStore = new KvStoreReaderImpl(file, config.passphrase.getOrElse(null), readOnly = false)
          val success = kvStore.checkAndFixFile()
          kvStore.close()
          if (success) {
            shouldDelete = false
            val ft = universe.fileManager().getFileType(file)
            finishedFile(file, ft)
          }
        }
        if (shouldDelete) {
          new File(config.folder, f).delete()
        }
      }
      new BlockingOperation()
    }
  }

  def usedIdentifiers(): Set[String] = {
//    randomAccessFile.synchronized {
      _usedIdentifiers
//    }
  }

  def isInconsistentBackup() = true

  def finishedFile(file: File, filetype: FileType[_], journalUpdate: Boolean = false): BlockingOperation = {
    checkLock()
    randomAccessFile.synchronized {
      val name = file.getName()
      randomAccessFile.seek(randomAccessFile.length())
      val command = "Finished"+(if (journalUpdate == true) updateMarker else "")+" "
      val line = command + name.length() + " " + name + "\r\n"
      _usedIdentifiers += name
      randomAccessFile.writeBytes(line)
      randomAccessFile.getFD().sync()
    }
    if (!filetype.isTemp(file) && !filetype.redundant) {
      universe.redundancyHandler.createPar2(file)
    }
    new BlockingOperation()
  }

  def shutdown(): BlockingOperation = {
    if (_open) {
      lock.release()
      randomAccessFile.close()
      _open = false
    }
    ret
  }

  def finish(): Boolean = {
    true
  }
  
  def createMarkerFile(writer: KvStoreWriter, filesToDelete: Seq[File]): BlockingOperation = {
    // TODO
//    writer.writeEntry(journalInZipFile) { out =>
//      out.write(filesToDelete.map(_.getName).mkString("\r\n").getBytes("UTF-8"))
//    }
    new BlockingOperation()
  }

}