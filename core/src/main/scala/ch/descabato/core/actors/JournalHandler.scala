package ch.descabato.core.actors

import java.io.{File, RandomAccessFile}
import java.nio.channels.FileLock

import ch.descabato.core_old.kvstore.KvStoreWriter
import ch.descabato.core_old.{BackupInUseException, BlockingOperation, FileType}
import ch.descabato.utils.Implicits._
import ch.descabato.utils.{Hash, Utils}

import scala.collection.immutable.HashSet

trait JournalHandler extends MyEventReceiver {

  // Removes all files not mentioned in the journal
  def cleanUnfinishedFiles(): BlockingOperation

  // All the identifiers that were once used and are now unsafe to use again
  def usedIdentifiers(): Set[String]

  // Reports whether the last shut down was dirty (crash)
  def isInconsistentBackup(): Boolean

  def startWriting(): BlockingOperation

  def stopWriting(): BlockingOperation
}

class JournalEntry(val typ: String) {
  override def toString: String = (typ.length + 1) + s" $typ"
}

class FileFinishedJournalEntry(typ: String, val file: String) extends JournalEntry(typ) {
  override def toString: String = (typ.length + 1 + file.length + 1) + s" $typ $file"
}

object JournalEntries {
  val fileFinishedTyp = "File_Finished"
  val startWritingTyp = "Start_Writing"
  val stopWritingTyp = "Stop_Writing"

  def fileFinished(file: String) = new FileFinishedJournalEntry(fileFinishedTyp, file: String)

  val startWriting = new JournalEntry(startWritingTyp)
  val stopWriting = new JournalEntry(stopWritingTyp)

  def parseLine(line: String): Option[JournalEntry] = {
    val num :: rest :: Nil = line.split(" ", 2).toList
    if (num.toInt == rest.length + 1) {
      rest.takeWhile(_ != ' ') match {
        case x if x == startWritingTyp =>
          return Some(startWriting)
        case x if x == stopWritingTyp =>
          return Some(stopWriting)
        case x if x == fileFinishedTyp =>
          val _ :: filename :: Nil = rest.split(" ", 2).toList
          return Some(fileFinished(filename))
      }
    }
    None
  }

}

// TODO some javadoc here
class SimpleJournalHandler(context: BackupContext) extends JournalHandler with Utils {

  import context._

  val journalInZipFile = "journalUpdates.txt"
  val updateMarker = "_withUpdate"

  lazy val journalName: String = config.prefix + "files-journal.txt"

  var backupClean = true

  private var _usedIdentifiers = HashSet[String]()

  var _open = true

  lazy val randomAccessFile = new RandomAccessFile(new File(config.folder, journalName), "rw")
  lazy val lock: FileLock = try {
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

  def cleanUnfinishedFiles(): BlockingOperation = {
    checkLock()
    var files = fileManager.allFiles().filter(_.isFile()).map(config.relativePath).toSet
    randomAccessFile.synchronized {
      randomAccessFile.seek(0)
      var line: String = null
      var backup = randomAccessFile.getFilePointer
      while ( {
        line = randomAccessFile.readLine(); line != null
      }) {
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
        l.debug(s"Deleting file $f because Journal does not mention it")
        new File(config.folder, f).delete()
      }
      new BlockingOperation()
    }
  }

  def usedIdentifiers(): Set[String] = {
    _usedIdentifiers
  }

  def isInconsistentBackup(): Boolean = !backupClean

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
      randomAccessFile.writeBytes(entry.toString + "\r\n")
      randomAccessFile.getFD().sync()
    }
  }

  def finishedFile(file: File, filetype: FileType[_], journalUpdate: Boolean = false, md5Hash: Hash): BlockingOperation = {
    checkLock()
    val entry = JournalEntries.fileFinished(config.relativePath(file))
    writeEntrySynchronized(entry)
    new BlockingOperation()
  }

  def finish(): Boolean = {
    if (_open) {
      lock.release()
      randomAccessFile.close()
      _open = false
    }
    true
  }

  def createMarkerFile(writer: KvStoreWriter, filesToDelete: Seq[File]): BlockingOperation = {
    // TODO
    //    writer.writeEntry(journalInZipFile) { out =>
    //      out.write(filesToDelete.map(_.getName).mkString("\r\n").getBytes("UTF-8"))
    //    }
    new BlockingOperation()
  }

  override def receive(myEvent: MyEvent): Unit = {
    myEvent match {
      case FileFinished(typ, file, tmp, hash) =>
        finishedFile(file, typ, tmp, hash)
    }

  }
}