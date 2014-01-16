package ch.descabato

import java.io.File
import java.util.{ HashMap => JHashMap }
import java.nio.file.Files
import java.nio.file.SimpleFileVisitor
import scala.collection.mutable.Buffer
import scala.collection.mutable
import backup.CompressionMode
import scala.collection.immutable.SortedMap
import org.scalatest.events.RecordableEvent
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

/**
 * The configuration to use when working with a backup folder.
 */
case class BackupFolderConfiguration(folder: File, passphrase: Option[String], newBackup: Boolean = false) {
  def serialization = new JsonSerialization()
  var keyLength = 1281
  var compressor = CompressionMode.none
  val hashLength = 16
  val hashAlgorithm = "MD5"
  def getMessageDigest = MessageDigest.getInstance(hashAlgorithm)
  val blockSize: Size = new Size("64Kb")
  val volumeSize: Size = new Size("64Mb")
  val useDeltas = true
}

/**
 * Loads a configuration and verifies the command line arguments
 */
class BackupConfigurationHandler(supplied: BackupFolderOption) {

  val mainFile = "backup.properties"
  val folder: File = new File(supplied.backupDestination())
  def hasOld = new File(folder, mainFile).exists()
  def loadOld() = new BackupFolderConfiguration(folder, None)
  def configure(): BackupFolderConfiguration = {
    if (hasOld) {
      val oldConfig = loadOld()
      merge(oldConfig, supplied)
    } else {
      new BackupFolderConfiguration(folder, None, true)
    }
  }

  def merge(old: BackupFolderConfiguration, newC: BackupFolderOption) = {
    old
  }

}

object BackupUtils {
  def findOld[T <: BackupPart](file: File, oldMap: mutable.Map[String, BackupPart])(implicit manifest: Manifest[T]): (Option[T]) = {
    val path = file.getAbsolutePath
    // if the file is in the map, no other file can have the same name. Therefore we remove it.
    val out = oldMap.remove(path)
    if (out.isDefined &&
      // file size has not changed
      out.get.size == file.length() &&
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

abstract class BackupIndexHandler extends Utils {
  val config: BackupFolderConfiguration

  lazy val fileManager = new FileManager(config)

  def loadOldIndex(): Buffer[BackupPart] = {
    val (filesToLoad, hasDeltas) = fileManager.getBackupAndUpdates
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

  def loadOldIndexAsMap(): mutable.Map[String, BackupPart] = {
    val buffer = loadOldIndex()
    val oldBackupFilesRemaining = mutable.HashMap[String, BackupPart]()
    buffer.foreach(x => oldBackupFilesRemaining += x.path -> x)
    oldBackupFilesRemaining
  }

  def partitionFolders(x: Seq[BackupPart]) = {
    val folders = Buffer[FolderDescription]()
    val files = Buffer[FileDescription]()
    x.foreach {
      case x: FolderDescription => folders += x
      case x: FileDescription => files += x
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
    val filesToLoad = fileManager.hashchains.getFiles()
    val list = filesToLoad.par.map(x => fileManager.hashchains.read(x)).fold(Buffer())(_ ++ _).seq
    val map = new HashChainMap
    map ++= list
    map
  }

  // TODO this is not needed for backups. Only for restores. For backups, only the keys are needed.
  var hashChainMap: HashChainMap = new HashChainMap()

  def importOldHashChains() {
    hashChainMap ++= oldBackupHashChains
  }

  def getHashChain(fd: FileDescription) = {
    if (fd.size <= config.blockSize.bytes) {
      fd.hash
    } else {
      hashChainMap.get(fd.hash).get
    }
  }

  def statistics(x: Seq[UpdatePart]) = {
    val num = x.size
    val totalSize = new Size(x.map(_.size).sum)
    f"$num%8d, $totalSize"
  }

}

class BackupHandler(val config: BackupFolderConfiguration)
  extends BackupDataHandler {
  self: BlockStrategy =>

  // Needed for backing up
  var hashChainMapTemp: HashChainMap = new HashChainMap()

  def backup(files: Seq[File]) {
    config.folder.mkdirs()
    val visitor = new OldIndexVisitor(loadOldIndexAsMap(), recordNew = true, recordUnchanged = true).walk(files)
    val (newParts, unchanged, deleted) = (visitor.newFiles, visitor.unchangedFiles, visitor.deleted)
    println("New Files      : " + statistics(newParts))
    println("Unchanged files: " + statistics(unchanged))
    println("Deleted files  : " + statistics(deleted))
    if ((newParts ++ deleted).isEmpty) {
      l.info("No files have been changed, aborting backup")
      return
    }
    val fileListOut = Buffer[BackupPart]()
    val (newFolders, newFiles) = newParts.partition(x => x.size == 0 && x.isInstanceOf[FolderDescription])
    importOldHashChains
    val saved = newFiles.collect { case x: FileDescription => x }.map(fileDesc => backupFileDesc(fileDesc))
    finishWriting
    if (!hashChainMapTemp.isEmpty)
    	fileManager.hashchains.write(hashChainMapTemp.toBuffer)
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
      if (!hashChainMap.contains(fileHash)) {
        hashChainMap += ((fileHash, hashList))
        hashChainMapTemp += ((fileHash, hashList))
      }
    }
    fileDesc.hash = fileHash
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
    val hashes = getHashChain(fd).grouped(config.getMessageDigest.getDigestLength()).toSeq
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