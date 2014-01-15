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

/**
 * The configuration to use when working with a backup folder.
 */
case class BackupFolderConfiguration(folder: File, passphrase: Option[String], newBackup: Boolean = false) {
  def serialization = new SmileSerialization()
  var keyLength = 128
  var compressor = CompressionMode.gzip
  val hashLength = 16
  val hashAlgorithm = "MD5"
  def getMessageDigest = MessageDigest.getInstance(hashAlgorithm)
  val blockSize: Size = new Size("64Kb")
  val volumeSize: Size = new Size("64Mb")
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
      // if the backup part is of the wrong type => return (None, fa)
      manifest.erasure.isAssignableFrom(out.get.getClass()) &&
      // if the file has attributes and the last modified date is different, return (None, fa)
      (out.get.attrs != null && !out.get.attrs.hasBeenModified(file))) {
      // backup part is correct and unchanged
      Some(out.get.asInstanceOf[T])
    } else {
      None
    }
  }
}

abstract class BackupIndexHandler(f: BackupFolderConfiguration) extends Utils {

  val fileManager = new FileManager(f)

  def loadOldIndex(): Buffer[BackupPart] = {
    fileManager.files.getFiles().map(fileManager.files.read).foldLeft(Buffer[BackupPart]())(_ ++ _)
  }

  def loadOldIndexAsMap(): mutable.Map[String, BackupPart] = {
    val buffer = loadOldIndex()
    val oldBackupFilesRemaining = mutable.HashMap[String, BackupPart]()
    buffer.foreach(x => oldBackupFilesRemaining += x.path -> x)
    oldBackupFilesRemaining
  }
}

abstract class BackupDataHandler(val config: BackupFolderConfiguration) extends BackupIndexHandler(config) {
  self: BlockStrategy =>
  import BAWrapper2.byteArrayToWrapper
  type HashChainMap = mutable.HashMap[BAWrapper2, Array[Byte]]

  def oldBackupHashChains: mutable.Map[BAWrapper2, Array[Byte]] = {
    val filesToLoad = fileManager.hashchains.getFiles()
    val list = filesToLoad.par.map(x => fileManager.hashchains.read(x)).fold(Buffer())(_ ++ _).seq
    val map = new HashChainMap
    map ++= list
    map
  }

  // TODO this is not needed for backups. Only for restores. For backups, only the keys are needed.
  var hashChainMap: HashChainMap = new HashChainMap()
  // Needed for backing up
  var hashChainMapTemp: HashChainMap = new HashChainMap()

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

  def statistics(x: Seq[BackupPart]) = {
    val num = x.size
    val totalSize = new Size(x.map(_.size).sum)
    f"$num%8d, $totalSize"
  }

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
    val fileListOut: Buffer[BackupPart] = unchanged
    val (newFolders, newFiles) = newParts.partition(x => x.size == 0 && x.isInstanceOf[FolderDescription])
    fileListOut ++= newFolders
    importOldHashChains
    val saved = newFiles.collect { case x: FileDescription => x }.map(fileDesc => backupFileDesc(fileDesc))
    finishWriting
    fileListOut ++= saved
    fileManager.hashchains.write(hashChainMapTemp.toBuffer)
    fileManager.files.write(fileListOut)
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

