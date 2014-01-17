package ch.descabato

import java.io.File
import java.util.{ HashMap => JHashMap }
import java.nio.file.Files
import java.nio.file.SimpleFileVisitor
import scala.collection.mutable.Buffer
import scala.collection.mutable
import backup.CompressionMode
import scala.collection.immutable.SortedMap
import Streams._
import java.io.FileInputStream
import java.security.MessageDigest
import java.util.Arrays
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.ByteArrayInputStream
import java.io.FileOutputStream
import java.io.SequenceInputStream
import java.util.Enumeration
import com.fasterxml.jackson.annotation.JsonIgnore
import java.io.IOException
import java.io.PrintStream
import java.util.Date
import java.security.DigestOutputStream

/**
 * The configuration to use when working with a backup folder.
 */
case class BackupFolderConfiguration(folder: File, prefix: String = "", @JsonIgnore var passphrase: Option[String] = None, newBackup: Boolean = false) {
  def this() = this(null)
  var version = 1
  var serializerType = "smile"
  @JsonIgnore
  def serialization = serializerType match {
    case "smile" => new SmileSerialization
    case "json" => new JsonSerialization
  }
  var keyLength = 128
  var compressor = CompressionMode.gzip
  def hashLength = getMessageDigest().getDigestLength()
  var hashAlgorithm = "MD5"
  @JsonIgnore def getMessageDigest() = MessageDigest.getInstance(hashAlgorithm)
  var blockSize: Size = Size("16Kb")
  var volumeSize: Size = Size("100Mb")
  val checkPointEvery: Size = volumeSize
  val useDeltas = false
  lazy val hasPassword = passphrase.isDefined
}

object InitBackupFolderConfiguration {
  def apply(option: BackupFolderOption) = {
    val out = BackupFolderConfiguration(option.backupDestination(), option.prefix(), option.passphrase.get)
    option match {
      case o: CreateBackupOptions =>
        o.serializerType.foreach(out.serializerType = _)
        o.compression.foreach(x => out.compressor = x)
        o.blockSize.foreach(out.blockSize = _)
        o.hashAlgorithm.foreach(out.hashAlgorithm = _)
      case o: ChangeableBackupOptions =>
        o.keylength.foreach(out.keyLength = _)
        o.volumeSize.foreach(out.volumeSize = _)
      case _ => // TODO
    }
    out
  }
}

/**
 * Loads a configuration and verifies the command line arguments
 */
class BackupConfigurationHandler(supplied: BackupFolderOption) {

  val mainFile = "backup.json"
  val folder: File = supplied.backupDestination()
  def hasOld = new File(folder, mainFile).exists()
  def loadOld() = {
    val json = new JsonSerialization()
    json.readObject[BackupFolderConfiguration](new FileInputStream(new File(folder, mainFile))).get
  }
  def configure(): BackupFolderConfiguration = {
    if (hasOld) {
      val oldConfig = loadOld()
      merge(oldConfig, supplied)
    } else {
      folder.mkdirs()
      val out = InitBackupFolderConfiguration(supplied)
      val json = new JsonSerialization()
      val fos = new FileOutputStream(new File(folder, mainFile))
      json.writeObject(out, fos)
      out
    }
  }

  def merge(old: BackupFolderConfiguration, supplied: BackupFolderOption) = {
    old.passphrase = supplied.passphrase.get
    supplied match {
      case o: ChangeableBackupOptions =>
        o.keylength.foreach(old.keyLength = _)
        o.volumeSize.foreach(old.volumeSize = _)
        // TODO other properties that can be set again
      case _ => 
    }
    println("After merge " + old)
    old
  }

}

object BackupUtils {
  def findOld[T <: BackupPart](file: File, oldMap: mutable.Map[String, BackupPart])(implicit manifest: Manifest[T]): (Option[T]) = {
    val path = file.getAbsolutePath
    // if the file is in the map, no other file can have the same name. Therefore we remove it.
    val out = oldMap.remove(path)
    if (out.isDefined &&
      // file size has not changed, if it is a file
      (file.isDirectory() || out.get.size == file.length()) &&
      // if the backup part is of the wrong type => return (None, fa)
      manifest.runtimeClass.isAssignableFrom(out.get.getClass()) &&
      // if the file has attributes and the last modified date is different, return (None, fa)
      (out.get.attrs != null && !out.get.attrs.hasBeenModified(file))) {
      // backup part is correct and unchanged
      Some(out.get.asInstanceOf[T])
    } else {
      None
    }
  }
}

trait BackupProgressReporting extends Utils {
  lazy val fileCounter = new StandardCounter("Files", 0) {}
  lazy val byteCounter = new StandardCounter("Data", 0) with ETACounter {
    override def format = s"${readableFileSize(current)}/${readableFileSize(maxValue)} ${percent}%"
  }

  def setMaximums(files: Int, bytes: Long) {
    fileCounter.maxValue = files
    byteCounter.maxValue = bytes
  }

  def updateProgress() {
    ProgressReporters.updateWithCounters(fileCounter :: byteCounter :: Nil)
  }
}

abstract class BackupIndexHandler extends Utils {
  val config: BackupFolderConfiguration

  lazy val fileManager = new FileManager(config)

  def loadOldIndex(temp: Boolean = false, date: Option[Date] = None): Buffer[BackupPart] = {
    val (filesToLoad, hasDeltas) = if (date.isDefined) {
      (fileManager.getBackupForDate(date.get).toArray, false)
    } else {
      fileManager.getBackupAndUpdates(temp)
    }
    val updates = filesToLoad.par.flatMap(fileManager.filesDelta.read).fold(Buffer[UpdatePart]())(_ ++ _).seq
    if (hasDeltas) {
      val map = mutable.LinkedHashMap[String, BackupPart]()
      updates.foreach {
        case FileDeleted(path) => map -= path
        case x: BackupPart => map(x.path) = x
      }
      map.values.toBuffer
    } else {
      updates.asInstanceOf[Buffer[BackupPart]]
    }
  }

  def loadOldIndexAsMap(temp: Boolean = false): mutable.Map[String, BackupPart] = {
    val buffer = loadOldIndex(temp)
    val oldBackupFilesRemaining = mutable.HashMap[String, BackupPart]()
    buffer.foreach(x => oldBackupFilesRemaining += x.path -> x)
    oldBackupFilesRemaining
  }

  def partitionFolders(x: Seq[BackupPart]) = {
    val folders = Buffer[FolderDescription]()
    val files = Buffer[FileDescription]()
    x.foreach {
      case x: FileDescription => files += x
      case x: FolderDescription => folders += x
    }
    (folders, files)
  }

}

abstract class BackupDataHandler extends BackupIndexHandler {
  self: BlockStrategy =>
  import BAWrapper2.byteArrayToWrapper
  type HashChainMap = mutable.HashMap[BAWrapper2, Array[Byte]]

  lazy val delta = config.useDeltas && !loadOldIndex().isEmpty

  def oldBackupHashChains: mutable.Map[BAWrapper2, Array[Byte]] = {
    def loadFor(x: Iterable[File]) = {
      x.par.flatMap(x => fileManager.hashchains.read(x)).fold(Buffer())(_ ++ _).toBuffer
    }
    val temps = loadFor(fileManager.hashchains.getTempFiles())
    if (!temps.isEmpty) {
      fileManager.hashchains.write(temps)
      fileManager.hashchains.deleteTempFiles()
    }
    val list = loadFor(fileManager.hashchains.getFiles())
    val map = new HashChainMap
    map ++= list
    map
  }

  // TODO this is not needed for backups. Only for restores. For backups, only the keys are needed.
  var hashChainMap: HashChainMap = new HashChainMap()

  def importOldHashChains() {
    hashChainMap ++= oldBackupHashChains
  }

  def hashChainSeq(a: Array[Byte]) = a.grouped(config.getMessageDigest.getDigestLength()).toSeq

  def getHashChain(fd: FileDescription): Seq[Array[Byte]] = {
    if (fd.size <= config.blockSize.bytes) {
      List(fd.hash)
    } else {
      hashChainSeq(hashChainMap.get(fd.hash).get)
    }
  }

  def statistics(x: Seq[BackupPart]) = {
    val num = x.size
    val totalSize = new Size(x.map(_.size).sum)
    var out = f"$num%8d, $totalSize"
    if ((1 to 10) contains num) {
      out += "\n" + x.map(_.path).mkString("\n")
    }
    out
  }

}

class BackupHandler(val config: BackupFolderConfiguration)
  extends BackupDataHandler with BackupProgressReporting {
  self: BlockStrategy =>

  // Needed for backing up
  var hashChainMapNew: HashChainMap = new HashChainMap()
  var hashChainMapCheckpoint: HashChainMap = new HashChainMap()

  def checkpoint(seq: Seq[FileDescription]) = {
    val (toSave, toKeep) = seq.partition(x => blockExists(getHashChain(x).last))
    fileManager.files.write(toSave.toBuffer, true)
    if (!hashChainMapCheckpoint.isEmpty) {
      val (toSave, toKeep) = hashChainMapCheckpoint.toBuffer.partition(x => blockExists(hashChainSeq(x._2).last))
      fileManager.hashchains.write(toSave.toBuffer, true)
      hashChainMapCheckpoint.clear
      hashChainMapCheckpoint ++= toKeep
    }
    toKeep
  }

  var failed = Buffer[FileDescription]()

  def backup(files: Seq[File]) {
    config.folder.mkdirs()

    // Walk tree and compile to do list

    val visitor = new OldIndexVisitor(loadOldIndexAsMap(true), recordNew = true, recordUnchanged = true).walk(files)
    val (newParts, unchanged, deleted) = (visitor.newFiles, visitor.unchangedFiles, visitor.deleted)
    println("New Files      : " + statistics(newParts))
    println("Unchanged files: " + statistics(unchanged))
    println("Deleted files  : " + statistics(deleted))
    if ((newParts ++ deleted).isEmpty) {
      l.info("No files have been changed, aborting backup")
      return
    }
    val fileListOut = Buffer[BackupPart]()
    val (newFolders, newFiles) = partitionFolders(newParts)

    // Backup files 

    setMaximums(newFiles.size, newFiles.map(_.size).sum)
    importOldHashChains
    var list = newFiles.toList
    var toSave: Seq[FileDescription] = Buffer.empty
    while (!list.isEmpty) {
      var sum = config.checkPointEvery.bytes
      val cur = list.takeWhile { x => val out = sum > 0; sum -= x.size; out }
      val temp = cur.flatMap(backupFileDesc(_))
      fileListOut ++= temp
      toSave ++= temp
      toSave = checkpoint(toSave)
      l.debug(s"Checkpointing: ${temp.size} files processed, ${toSave.size} pending to be saved")
      list = list.drop(cur.size)
    }

    // Clean up, handle failed entries

    if (!failed.isEmpty) {
      l.info(failed.size + " files could not be backed up due to file locks, trying again.")
      val copy = failed.toBuffer
      failed.clear()
      val succeeded = copy.flatMap(fileDesc => backupFileDesc(fileDesc))
      if (succeeded.size == copy.size) {
        l.info("All files backed up now")
      } else {
        l.info(copy.size - succeeded.size + " Files could not be backed up. See log for a complete list")
        failed.foreach { f => l.debug("File " + f + " was not backed up due to locks") }
      }
    }

    // finish writing, complete the backup

    finishWriting
    if (!hashChainMapNew.isEmpty) {
      fileManager.hashchains.write(hashChainMapNew.toBuffer)
      fileManager.hashchains.deleteTempFiles()
    }
    if (!delta) {
      fileListOut ++= unchanged
    }
    fileListOut ++= newFolders
    if (delta) {
      val toWrite = deleted.map { case x: BackupPart => FileDeleted(x.path) }.toBuffer ++ fileListOut
      fileManager.filesDelta.write(toWrite)
    } else {
      fileManager.files.write(fileListOut)
      fileManager.files.deleteTempFiles()
    }
    l.info("Backup completed")
  }

  def backupFileDesc(fileDesc: FileDescription): Option[FileDescription] = {
    val byteCounterbackup = byteCounter.current
    try {
      // This is a new file, so we start hashing its contents, fill those in and return the same instance
      val file = new File(fileDesc.path)
      val fis = new FileInputStream(file)
      val (fileHash, hashList) = ObjectPools.baosPool.withObject((), {
        out: ByteArrayOutputStream =>
          val md = config.getMessageDigest
          def hashAndWriteBlock(buf: Array[Byte]) {
            val hash = md.digest(buf)
            out.write(hash)
            if (!blockExists(hash)) {
              writeBlock(hash, buf)
            }
            byteCounter += buf.size
            updateProgress
          }
          val hash = {
            val blockHasher = new BlockOutputStream(config.blockSize.bytes.toInt, hashAndWriteBlock _)
            val hos = new HashingOutputStream(config.hashAlgorithm)
            val sis = new SplitInputStream(fis, blockHasher :: hos :: Nil)
            sis.readComplete
            sis.close()
            hos.out.get
          }
          (hash, out.toByteArray())
      })
      if (hashList.length == fileHash.length) {
        null
      } else {
        // use same instance for the keys
        val key: BAWrapper2 = fileHash
        if (!hashChainMap.contains(key)) {
          hashChainMap += ((key, hashList))
          hashChainMapNew += ((key, hashList))
          hashChainMapCheckpoint += ((key, hashList))
        }
      }
      fileDesc.hash = fileHash
      fileCounter += 1
      updateProgress()
      Some(fileDesc)
    } catch {
      case io: IOException if (io.getMessage().contains("The process cannot access the file")) => {
        failed += fileDesc
        l.warn("Could not backup file " + fileDesc.path + " because it is locked.")
        None
      }
      case e: IOException => throw e
    }
  }

}

class RestoreHandler(val config: BackupFolderConfiguration)
  extends BackupDataHandler with BackupProgressReporting {
  self: BlockStrategy =>

  val hashChains = importOldHashChains()

  def restore(options: RestoreConf, d: Option[Date] = None) {
    implicit val o = options
    val filesInBackup = loadOldIndex(date = d)
    val filtered = if (o.pattern.isDefined) filesInBackup.filter(x => x.path.contains(options.pattern())) else filesInBackup
    l.info("Going to restore " + statistics(filtered))
    setMaximums(filtered.size, filtered.map(_.size).sum)
    val (folders, files) = partitionFolders(filtered)
    folders.foreach(restoreFolderDesc)
    files.foreach(restoreFileDesc)
    folders.foreach(restoreFolderDesc)
    free()
  }

  def restoreFromDate(t: RestoreConf, d: Date) {
    restore(t, Some(d))
  }

  def makePath(fd: BackupPart)(implicit options: RestoreConf) = {
    val dest = new File(options.restoreToFolder())
    def cleaned(s: String) = if (Utils.isWindows) s.replaceAllLiterally(":", "_") else s
    if (options.relativeToFolder.isDefined) {
      new File(dest, fd.relativeTo(new File(options.relativeToFolder())))
    } else {
      new File(dest, cleaned(fd.path))
    }

  }

  def restoreFolderDesc(fd: FolderDescription)(implicit options: RestoreConf) {
    val restoredFile = makePath(fd)
    restoredFile.mkdirs()
    fd.applyAttrsTo(restoredFile)
    fileCounter += 1
    updateProgress
  }

  def getInputStream(fd: FileDescription): InputStream = {
    val hashes = getHashChain(fd)
    val enumeration = new Enumeration[InputStream]() {
      val hashIterator = hashes.iterator
      def hasMoreElements = hashIterator.hasNext
      def nextElement = readBlock(hashIterator.next)
    }
    new SequenceInputStream(enumeration)
  }

  def restoreFileDesc(fd: FileDescription)(implicit options: RestoreConf) {
    try {
      val restoredFile = makePath(fd)
      if (restoredFile.exists()) {
        if (restoredFile.length() == fd.size && !fd.attrs.hasBeenModified(restoredFile)) {
          fileCounter += 1
          byteCounter += fd.size
          return
        }
        l.debug(s"${restoredFile.length()} ${fd.size} ${fd.attrs} ${restoredFile.lastModified()}")
        l.warn("File exists, but has been modified, so overwrite")
      }
      val fos = new FileOutputStream(restoredFile)
      val dos = new DigestOutputStream(fos, config.getMessageDigest)
      copy(getInputStream(fd), dos)
      val hash = dos.getMessageDigest().digest()
      if (!Arrays.equals(fd.hash, hash)) {
        l.warn("Error while restoring file, hash is not correct")
      }
      if (restoredFile.length() != fd.size) {
        l.warn(s"Restoring failed for $restoredFile (old size: ${fd.size}, now: ${restoredFile.length()}")
      }
      fd.applyAttrsTo(restoredFile)
    } catch {
      case e: Exception =>
        l.warn("Exception while restoring " + fd.path + " (" + e.getMessage() + ")")
        logException(e)
    }
    fileCounter += 1
    byteCounter += fd.size
    updateProgress
  }

}

class VfsIndex(config: BackupFolderConfiguration)
  extends RestoreHandler(config) {
  self: BlockStrategy =>

  val files = loadOldIndex()

  def registerIndex() {
    l.info("Loading information")
    val path = config.folder.getAbsolutePath()
    l.info("Path is loaded as " + path)
    BackupVfsProvider.indexes += Utils.normalizePath(path) -> this
  }

}