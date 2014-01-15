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

/**
 * The configuration to use when working with a backup folder.
 */
case class BackupFolderConfiguration(folder: File, passphrase: Option[String], newBackup: Boolean = false) {
  def serialization = new SmileSerialization()
  var keyLength = 128
  var compressor = CompressionMode.gzip
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

class BackupDataHandler(f: BackupFolderConfiguration) extends BackupIndexHandler(f) {
  //  self: BlockStrategy =>
  import BAWrapper2.byteArrayToWrapper
  type HashChainMap = mutable.HashMap[BAWrapper2, Array[Byte]]

  def backup(files: Seq[File]) {
    f.folder.mkdirs()
    val visitor = new OldIndexVisitor(loadOldIndexAsMap(), recordNew = true, recordUnchanged = true).walk(files)
    val (newFiles, unchanged, deleted) = (visitor.newFiles, visitor.unchangedFiles, visitor.deleted)
//    println("New Files "+newFiles)
//    println("unchanged "+unchanged)
//    println("deleted "+deleted)
    if ((newFiles++deleted).isEmpty) {
      l.info("No files have been changed, aborting backup")
      return 
    }
    val fileListOut = unchanged
    fileListOut ++= newFiles
    fileManager.files.write(fileListOut)
  }

}

