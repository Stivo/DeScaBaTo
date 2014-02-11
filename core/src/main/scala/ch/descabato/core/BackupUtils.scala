package ch.descabato.core

import scala.collection.mutable
import java.io.File
import ch.descabato.utils.Utils
import ch.descabato.frontend.StandardCounter
import ch.descabato.frontend.StandardMaxValueCounter
import ch.descabato.frontend.ETACounter
import ch.descabato.frontend.ProgressReporters

object BackupUtils {
  def findOld[T <: BackupPart](file: File, oldMap: mutable.Map[String, BackupPart])(implicit manifest: Manifest[T]): (Option[T]) = {
    val path = file.getCanonicalPath
    // if the file is in the map, no other file can have the same name. Therefore we remove it.
    val out = oldMap.remove(path)
    if (out.isDefined &&
      // file size has not changed, if it is a file
      (!(out.get.isInstanceOf[FileDescription]) || out.get.size == file.length()) &&
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
  def filecountername = "Files Read"
  def bytecountername = "Data Read"
  lazy val scanCounter = new StandardCounter("Files found: ")
  lazy val fileCounter = new StandardMaxValueCounter(filecountername, 0) {}
  lazy val byteCounter = new StandardMaxValueCounter(bytecountername, 0) with ETACounter {
    override def formatted = s"${readableFileSize(current)}/${readableFileSize(maxValue)} $percent%"
  }

  def setMaximums(desc: BackupDescription) {
    ProgressReporters.openGui()
    fileCounter.maxValue = desc.files.size
    byteCounter.maxValue = desc.files.map(_.size).sum
    fileCounter.current = 0
    byteCounter.current = 0
  }

  def updateProgress() {
    ProgressReporters.updateWithCounters(fileCounter :: byteCounter :: Nil)
  }
}