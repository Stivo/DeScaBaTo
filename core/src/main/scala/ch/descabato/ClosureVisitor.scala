package ch.descabato

import java.io.File
import java.nio.file.attribute.BasicFileAttributes
import scala.collection.mutable
import java.io.IOException
import scala.collection.mutable.Buffer
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import ch.descabato.ProgressReporters

class ClosureVisitor(f: (File, BasicFileAttributes) => Unit) extends FileVisitorHelper {

  override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes) = {
    f(dir.toFile(), attrs)
    super.preVisitDirectory(dir, attrs)
  }

  override def visitFile(file: Path, attrs: BasicFileAttributes) = {
    f(file.toFile(), attrs)
    super.visitFile(file, attrs)
  }

  override def visitFileFailed(file: Path, exc: IOException) = {
    FileVisitResult.CONTINUE
  }

}

class OldIndexVisitor(oldMap: mutable.Map[String, BackupPart],
  recordNew: Boolean = false, recordAll: Boolean = false, recordUnchanged: Boolean = false,
  progress: Option[Counter] = None) extends FileVisitorHelper {

  val all = Buffer[BackupPart]()
  lazy val deleted = oldMap.values.toSeq
  val newFiles = Buffer[BackupPart]()
  val unchangedFiles = Buffer[BackupPart]()

  def handleFile[T <: BackupPart](f: => T, dir: Path, attrs: BasicFileAttributes)(implicit m: Manifest[T]) {
    lazy val desc = f
    var wasadded = false
    val old = BackupUtils.findOld(dir.toFile(), oldMap)(m)
    if (old.isDefined) {
      if (recordUnchanged) {
        unchangedFiles += old.get
      }
      if (recordAll) {
        wasadded = true
        all += old.get
      }
    } else if (recordNew) {
      newFiles += desc
    }
    if (recordAll && !wasadded) {
      all += desc
    }
    progress.foreach{x =>
      x += 1
  	  ProgressReporters.updateWithCounters(List(x))
    }
  }

  override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes) = {
    handleFile(new FolderDescription(dir.toAbsolutePath().toString(), FileAttributes.convert(attrs)),
      dir, attrs)
    super.preVisitDirectory(dir, attrs)
  }

  override def visitFile(file: Path, attrs: BasicFileAttributes) = {
    handleFile({
      val out = new FileDescription(file.toAbsolutePath().toString(), file.toFile().length(),
        FileAttributes.convert(attrs))
      out.hash = "asdf".getBytes()
      out
    },
      file, attrs)
    super.visitFile(file, attrs)
  }

  override def visitFileFailed(file: Path, exc: IOException) = {
    FileVisitResult.CONTINUE
  }

  def walk(f: Seq[File]) = {
    f.foreach { x =>
      Files.walkFileTree(x.toPath(), this)
    }
    this
  }

}