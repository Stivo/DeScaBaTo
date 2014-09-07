package ch.descabato.core

import java.io.File
import java.io.IOException
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes

import ch.descabato.FileVisitorHelper
import ch.descabato.frontend.Counter

class OldIndexVisitor(var oldMap: Map[String, BackupPart],
  recordNew: Boolean = false, recordAll: Boolean = false, recordUnchanged: Boolean = false,
  progress: Option[Counter] = None) extends FileVisitorHelper {

  var allDesc = new BackupDescription()
  var newDesc = new BackupDescription()
  var unchangedDesc = new BackupDescription()
  lazy val deletedDesc = {
    new BackupDescription(deleted = oldMap.values.map(x => new FileDeleted(x.path)).toVector)
  }
  
  val symManifest = manifest[SymbolicLink]

  def handleFile[T <: BackupPart](f: => T, dir: Path, attrs: BasicFileAttributes)(implicit m: Manifest[T]) {
    lazy val desc = f
    var wasadded = false
    val (mapNew, old) = if (Files.isSymbolicLink(dir))
      BackupUtils.findOld[SymbolicLink](dir.toRealPath().toFile(), oldMap)(symManifest)
    else
      BackupUtils.findOld(dir.toRealPath().toFile(), oldMap)(m)
    oldMap = mapNew
    if (old.isDefined) {
      if (recordUnchanged) {
        unchangedDesc += old.get
      }
      if (recordAll) {
        wasadded = true
        allDesc += old.get
      }
    } else if (recordNew) {
      newDesc += desc
    }
    if (recordAll && !wasadded) {
      allDesc += desc
    }
    progress.foreach { x =>
      x += 1
    }
  }

  override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes) = {
    handleFile(new FolderDescription(dir.toRealPath().toString(), FileAttributes(dir)),
      dir, attrs)
    super.preVisitDirectory(dir, attrs)
  }

  override def visitFile(file: Path, attrs: BasicFileAttributes) = {
    handleFile({
      if (Files.isSymbolicLink(file)) {
        new SymbolicLink(file.toRealPath(LinkOption.NOFOLLOW_LINKS).toString(),
          file.getParent.resolve(Files.readSymbolicLink(file)).toRealPath().toString(), FileAttributes(file))
      } else {
        val out = new FileDescription(file.toRealPath().toString(), file.toFile().length(),
          FileAttributes(file))
        out
      }
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