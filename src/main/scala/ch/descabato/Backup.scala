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

/**
 * The configuration to use when working with a backup folder.
 */
case class BackupFolderConfiguration(folder: File, @JsonIgnore var passphrase: Option[String] = None, newBackup: Boolean = false) {
  def this() = this(null)
  var version = 1
  var serializerType = "smile"
  @JsonIgnore
  def serialization = serializerType match {
    case "smile" => new SmileSerialization
    case "json" => new JsonSerialization
  }
  var keyLength = 192
  var compressor = CompressionMode.none
  def hashLength = getMessageDigest().getDigestLength()
  val hashAlgorithm = "MD5"
  @JsonIgnore def getMessageDigest() = MessageDigest.getInstance(hashAlgorithm)
  val blockSize: Size = Size("64Kb")
  val volumeSize: Size = Size("64Mb")
  val checkPointEvery: Size = volumeSize
  val useDeltas = false
  lazy val hasPassword = passphrase.isDefined
}

object InitBackupFolderConfiguration {
  def apply(option: BackupFolderOption) = {
    val out = BackupFolderConfiguration(new File(option.backupDestination()).getAbsoluteFile(), option.passphrase.get)
    option match {
      case o : CreateBackupOptions => 
        o.serializerType.foreach(out.serializerType = _)
        o.compression.foreach(x => out.compressor = CompressionMode.valueOf(x.toLowerCase))
    }
    out
  }
}

/**
 * Loads a configuration and verifies the command line arguments
 */
class BackupConfigurationHandler(supplied: BackupFolderOption) {

  val mainFile = "backup.json"
  val folder: File = new File(supplied.backupDestination())
  def hasOld = new File(folder, mainFile).exists()
  def loadOld() = {
    val json = new JsonSerialization()
    json.readObject[BackupFolderConfiguration](new FileInputStream(new File(folder, mainFile)))
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
    // TODO other properties that can be set again
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
    ProgressReporters.updateWithCounters(fileCounter::byteCounter::Nil)
  }
}

abstract class BackupIndexHandler extends Utils {
  val config: BackupFolderConfiguration

  lazy val fileManager = new FileManager(config)

  def loadOldIndex(temp: Boolean = false): Buffer[BackupPart] = {
    val (filesToLoad, hasDeltas) = fileManager.getBackupAndUpdates(temp)
    val updates = filesToLoad.par.map(fileManager.filesDelta.read).fold(Buffer[UpdatePart]())(_ ++ _).seq
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
      x.par.map(x => fileManager.hashchains.read(x)).fold(Buffer())(_ ++ _).toBuffer
    }
    val temps = loadFor(fileManager.hashchains.getTempFiles())
    if (!temps.isEmpty) {
      fileManager.hashchains.write(temps)
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
  
  def getHashChain(fd: FileDescription) : Seq[Array[Byte]] = {
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
      out += "\n"+x.map(_.path).mkString("\n")
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
     l.info("Checkpointing ")
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
  
  def backup(files: Seq[File]) {
    config.folder.mkdirs()
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

    setMaximums(newFiles.size, newFiles.map(_.size).sum)
    importOldHashChains
    var list = newFiles.toList
    var toSave: Seq[FileDescription] = Buffer.empty
    while (!list.isEmpty) {
      var sum = config.checkPointEvery.bytes
      val cur = list.takeWhile{x => val out = sum > 0; sum -= x.size; out}
      val temp = cur.map(backupFileDesc)
      l.info("Handled "+cur.size+" before checkpointing")
      toSave ++= temp
      toSave = checkpoint(toSave)
      list = list.drop(cur.size)
    }
    val saved = newFiles.map(fileDesc => backupFileDesc(fileDesc))
    finishWriting
    if (!hashChainMapNew.isEmpty) {
      fileManager.hashchains.write(hashChainMapNew.toBuffer)
      fileManager.hashchains.deleteTempFiles()
    }
    if (!delta) {
      fileListOut ++= unchanged
    }
    fileListOut ++= newFolders
    fileListOut ++= saved
    if (delta) {
      val toWrite = deleted.map { case x: BackupPart => FileDeleted(x.path) }.toBuffer ++ fileListOut
      fileManager.filesDelta.write(toWrite)
    } else {
      fileManager.files.write(fileListOut)
      fileManager.files.deleteTempFiles()
    }
  }

  def backupFileDesc(fileDesc: FileDescription) = {
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
      val key : BAWrapper2 = fileHash
      if (!hashChainMap.contains(key)) {
        hashChainMap += ((key, hashList))
        hashChainMapNew += ((key, hashList))
        hashChainMapCheckpoint += ((key, hashList))
      }
    }
    fileDesc.hash = fileHash
    fileCounter += 1
    updateProgress()
    fileDesc
  }

}

class RestoreHandler(val config: BackupFolderConfiguration)
  extends BackupDataHandler {
  self: BlockStrategy =>

  val hashChains = importOldHashChains()

  def restore(options: RestoreConf) {
    implicit val o = options
    val filesInBackup = loadOldIndex()
    val filtered = if (o.pattern.isDefined) filesInBackup.filter(x => x.path.contains(options.pattern())) else filesInBackup
    println("Going to restore " + statistics(filtered))
    val (folders, files) = partitionFolders(filtered)
    folders.foreach(restoreFolderDesc)
    files.foreach(restoreFileDesc)
    folders.foreach(restoreFolderDesc)
    free()
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
    val restoredFile = makePath(fd)
    if (restoredFile.exists()) {
      if (restoredFile.length() == fd.size && !fd.attrs.hasBeenModified(restoredFile)) {
        return
      }
      l.info(s"${restoredFile.length()} ${fd.size} ${fd.attrs} ${restoredFile.lastModified()}")
      l.info("File exists, but has been modified, so overwrite")
    }
    l.info("Restoring to " + restoredFile)
    val fos = new FileOutputStream(restoredFile)
    copy(getInputStream(fd), fos)
    if (restoredFile.length() != fd.size) {
      l.error(s"Restoring failed for $restoredFile (old size: ${fd.size}, now: ${restoredFile.length()}")
    }
    fd.applyAttrsTo(restoredFile)
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