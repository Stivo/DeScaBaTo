package ch.descabato.core.actors

import java.io.{File, RandomAccessFile}
import java.nio.channels.FileLock

import ch.descabato.core_old.{BackupInUseException, FileType}
import ch.descabato.utils.Implicits._
import ch.descabato.utils.{Hash, Utils}

import scala.collection.immutable.HashSet

class BlockingOperation

trait JournalHandler extends MyEventReceiver {
  def getMd5ForFile(file: File): Option[Hash]

  // Removes all files not mentioned in the journal
  def cleanUnfinishedFiles(): BlockingOperation

  // All the identifiers that were once used and are now unsafe to use again
  def usedIdentifiers(): Set[String]

  // Reports whether the last shut down was dirty (crash)
  def isInconsistentBackup(): Boolean

  def startWriting(): BlockingOperation

  def stopWriting(): BlockingOperation

  def finish(): Boolean
}

class JournalEntry(val typ: String) {
  override def toString: String = (typ.length + 1) + s" $typ"
}

case class FileFinishedJournalEntry(override val typ: String, val file: String, val md5Hash: Hash) extends JournalEntry(typ) {
  override def toString: String = {
    val value = s"$typ $file ${md5Hash.base64}"
    (value.length + 1) + " " + value
  }
}

object JournalEntries {
  val fileFinishedTyp = "File_Finished"
  val startWritingTyp = "Start_Writing"
  val stopWritingTyp = "Stop_Writing"

  def fileFinished(file: String, hash: Hash) = new FileFinishedJournalEntry(fileFinishedTyp, file, hash)

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
          val _ :: filename :: hash :: Nil = rest.split(" ", 3).toList
          return Some(fileFinished(filename, Hash.fromBase64(hash)))
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

  private var entries: Seq[JournalEntry] = Seq.empty

  private def parseFile(): Unit = {
    randomAccessFile.synchronized {
      randomAccessFile.seek(0)
      var line: String = null
      var backup = randomAccessFile.getFilePointer
      while ( {
        line = randomAccessFile.readLine()
        line != null
      }) {
        if (line != null) {
          val maybeEntry = JournalEntries.parseLine(line)
          maybeEntry.foreach(entries :+= _)
        }
      }
      entries.foreach {
        case FileFinishedJournalEntry(_, file, _) => _usedIdentifiers += file
        case _ =>
      }
    }
  }
  parseFile()

  def cleanUnfinishedFiles(): BlockingOperation = {
    checkLock()
    var files = fileManager.allFiles().filter(_.isFile()).map(config.relativePath).toSet
    for (entry <- entries) {
      entry match {
        case x: FileFinishedJournalEntry =>
          val f = x.file
          _usedIdentifiers += f
          if (!(files safeContains f)) {
            if (!(f contains "temp."))
              logger.warn("File is in journal, but not on disk " + f)
          } else {
            files -= f
          }
        case x if x.typ == JournalEntries.startWritingTyp =>
          backupClean = false
        case x if x.typ == JournalEntries.stopWritingTyp =>
          backupClean = true
      }
    }

    files.foreach { f =>
      logger.info(s"Deleting file $f because Journal does not mention it")
      new File(config.folder, f).delete()
    }
    new BlockingOperation()
  }

  def getMd5ForFile(file: File): Option[Hash] = {
    val str = config.relativePath(file)
    entries.collectFirst {
      case FileFinishedJournalEntry(_, fileInJournal, hash) if fileInJournal == str => hash
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

  def finishedFile(file: File, filetype: FileType, journalUpdate: Boolean = false, md5Hash: Hash): BlockingOperation = {
    checkLock()
    val entry = JournalEntries.fileFinished(config.relativePath(file), md5Hash)
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

  override def receive(myEvent: MyEvent): Unit = {
    myEvent match {
      case FileFinished(typ, file, tmp, hash) =>
        finishedFile(file, typ, !tmp, hash)
    }

  }
}