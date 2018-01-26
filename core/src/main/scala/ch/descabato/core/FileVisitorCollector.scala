package ch.descabato.core

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, FileVisitor, Path}

import ch.descabato.frontend.MaxValueCounter

class FileVisitorCollector(fileCounter: MaxValueCounter, bytesCounter: MaxValueCounter) extends FileVisitor[Path] {
  // TODO log exceptions
  private var _files: Seq[Path] = Seq.empty
  private var _dirs: Seq[Path] = Seq.empty
  override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
    FileVisitResult.CONTINUE
  }

  override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
    _files :+= file
    fileCounter.maxValue += 1
    bytesCounter.maxValue += file.toFile.length()
    FileVisitResult.CONTINUE
  }

  override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = {
    FileVisitResult.CONTINUE
  }

  override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
    _dirs :+= dir
    FileVisitResult.CONTINUE
  }

  def files: Seq[Path] = _files
  def dirs: Seq[Path] = _dirs
}
