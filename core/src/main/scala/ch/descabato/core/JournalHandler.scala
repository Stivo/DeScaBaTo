package ch.descabato.core

import java.io.File
import java.io.RandomAccessFile

import ch.descabato.core.kvstore.KvStoreReaderImpl
import ch.descabato.core.kvstore.KvStoreWriter
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Utils

import scala.collection.immutable.HashSet

class BlockingOperation

class JournalEntry(val typ: String) {
  override def toString = (typ.length + 1) + s" $typ"
}

class FileFinishedJournalEntry(typ: String, val file: String) extends JournalEntry(typ) {
  override def toString = (typ.length + 1 + file.length + 1) + s" $typ $file"
}

object JournalEntries {
  val fileFinishedTyp = "File_Finished"
  val startWritingTyp = "Start_Writing"
  val stopWritingTyp = "Stop_Writing"
  def fileFinished(file: String) = new FileFinishedJournalEntry(fileFinishedTyp, file: String)
  val startWriting = new JournalEntry(startWritingTyp)
  val stopWriting = new JournalEntry(stopWritingTyp)
  def parseLine(line: String): Option[JournalEntry] = {
    val num::rest::Nil = line.split(" ", 2).toList
    if (num.toInt == rest.length + 1) {
      rest.takeWhile(_!=' ') match {
        case x if x == startWritingTyp =>
          return Some(startWriting)
        case x if x == stopWritingTyp =>
          return Some(stopWriting)
        case x if x == fileFinishedTyp =>
          val _::filename::Nil = rest.split(" ", 2).toList
          return Some(fileFinished(filename))
      }
    }
    None
  }

}

// TODO some javadoc here
class SimpleJournalHandler extends JournalHandler with Utils {

  val journalInZipFile = "journalUpdates.txt"
  val updateMarker = "_withUpdate"

  lazy val journalName = config.prefix + "files-journal.txt"

  var backupClean = true

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
      var backup = randomAccessFile.getFilePointer
      while ({ line = randomAccessFile.readLine(); line != null }) {
        if (line != null) {
          JournalEntries.parseLine(line) match {
            case Some(x: FileFinishedJournalEntry) =>
              val f = x.file
              _usedIdentifiers += f
              if (!(files safeContains f)) {
                if (!(f contains "temp."))
                  l.warn("File is in journal, but not on disk " + f)
              } else {
                files -= f
              }
            case Some(x) if x.typ == JournalEntries.startWritingTyp =>
              backupClean = false
            case Some(x) if x.typ == JournalEntries.stopWritingTyp =>
              backupClean = true
            case None =>
              randomAccessFile.setLength(backup)
          }
          backup = randomAccessFile.getFilePointer
        }
      }
      files.foreach { f =>
        l.debug(s"Checking file $f because Journal does not mention it")
        val file = new File(config.folder, f)
        var shouldDelete = true
        if (file.getName.endsWith(".kvs")) {
          val kvStore = new KvStoreReaderImpl(file, config.passphrase.orNull, readOnly = false)
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
      _usedIdentifiers
  }

  def isInconsistentBackup() = !backupClean

  def startWriting(): BlockingOperation = {
    if (backupClean) {
      backupClean = false
      writeEntrySynchronized(JournalEntries.startWriting)
    }
    new BlockingOperation()
  }
  def stopWriting(): BlockingOperation = {
    backupClean = true
    writeEntrySynchronized(JournalEntries.stopWriting)
    new BlockingOperation()
  }

  def writeEntrySynchronized(entry: JournalEntry) {
    randomAccessFile.synchronized {
      randomAccessFile.seek(randomAccessFile.length())
      randomAccessFile.writeBytes(entry.toString+"\r\n")
      randomAccessFile.getFD().sync()
    }
  }

  def finishedFile(file: File, filetype: FileType[_], journalUpdate: Boolean = false): BlockingOperation = {
    checkLock()
    val entry = JournalEntries.fileFinished(file.getName)
    writeEntrySynchronized(entry)
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