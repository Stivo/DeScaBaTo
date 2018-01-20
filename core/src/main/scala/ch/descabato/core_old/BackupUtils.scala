package ch.descabato.core_old

import java.io.File

import ch.descabato.frontend.{ETACounter, ProgressReporters, StandardCounter, StandardMaxValueCounter}
import ch.descabato.utils.Utils

object BackupUtils {
  def findOld[T <: BackupPart](file: File, oldMap: Map[String, BackupPart])(implicit manifest: Manifest[T]): (Map[String, BackupPart], Option[T]) = {
    val path = file.getCanonicalPath
    // if the file is in the map, no other file can have the same name. Therefore we remove it.
    val out = oldMap.get(path)
    if (out.isDefined &&
      // file size has not changed, if it is a file
      (!(out.get.isInstanceOf[FileDescription]) || out.get.size == file.length()) &&
      // if the backup part is of the wrong type => return (None, fa)
      manifest.runtimeClass.isAssignableFrom(out.get.getClass()) &&
      // if the file has attributes and the last modified date is different, return (None, fa)
      (out.get.attrs != null && !out.get.attrs.hasBeenModified(file))) {
      // backup part is correct and unchanged
      (oldMap - path, Some(out.get.asInstanceOf[T]))
    } else {
      (oldMap, None)
    }
  }
}

trait BackupProgressReporting extends Utils {
  protected def config: BackupFolderConfiguration
  def filecountername = "Files Read"
  def bytecountername = "Data Read"
  lazy val scanCounter = new StandardCounter("Files found: ")
  lazy val fileCounter: StandardMaxValueCounter = new StandardMaxValueCounter(filecountername, 0) {}
  lazy val byteCounter: StandardMaxValueCounter with ETACounter = new StandardMaxValueCounter(bytecountername, 0) with ETACounter {
    override def formatted = s"${readableFileSize(current)}/${readableFileSize(maxValue)} $percent%"
  }
  lazy val failureCounter: StandardMaxValueCounter = new StandardMaxValueCounter("Failed files", 0) {}

  def setMaximums(desc: BackupDescription, withGui: Boolean = false) {
    setMaximums(desc.files, withGui)
  }

  def nameOfOperation: String

  def setMaximums(seq: Seq[FileDescription], withGui: Boolean) {
    if (withGui) {
      ProgressReporters.openGui(nameOfOperation, config.threads == 1, config.remoteOptions)
    }
    fileCounter.maxValue = seq.size
    byteCounter.maxValue = seq.map(_.size).sum
    fileCounter.current = 0
    byteCounter.current = 0
    failureCounter.maxValue = seq.size
    registerCounters()
  }

  def registerCounters() {
    ProgressReporters.addCounter(scanCounter, fileCounter, byteCounter, failureCounter)
  }

}
