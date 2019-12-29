package ch.descabato.core

import java.io.File
import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.{ArrayList => JArrayList}

import ch.descabato.frontend.MaxValueCounter
import ch.descabato.frontend.StandardMaxValueCounter
import ch.descabato.utils.Utils

import scala.collection.JavaConverters._
import scala.io.Codec
import scala.io.Source

class FileVisitorCollector(ignoreFile: Option[File],
                           fileCounter: MaxValueCounter = new StandardMaxValueCounter("files", 0),
                           bytesCounter: MaxValueCounter = new StandardMaxValueCounter("bytes", 0)) extends Utils {
  // TODO log exceptions
  private var _files: JArrayList[Path] = new JArrayList[Path]()
  private var _dirs: JArrayList[Path] = new JArrayList[Path]()

  def walk(path: Path): Unit = {
    val visitor = new FileVisitor(path)
    Files.walkFileTree(path, visitor)
  }

  private val ignoreFileMatcher = new IgnoreFileMatcher(ignoreFile)

  class FileVisitor(root: Path) extends SimpleFileVisitor[Path] {

    override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
      FileVisitResult.CONTINUE
    }

    def pathIsNotIgnored(file: Path): Boolean = {
      ignoreFileMatcher.pathIsNotIgnored(root, file)
    }

    override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
      // TODO handle symbolic links
      if (pathIsNotIgnored(file) && !Files.isSymbolicLink(file)) {
        _files.add(file)
        fileCounter.maxValue += 1
        bytesCounter.maxValue += file.toFile.length()
      }
      FileVisitResult.CONTINUE
    }

    override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = {
      logger.warn(s"Could not visit file $file, due to ${exc.getClass}: ${exc.getMessage}")
      FileVisitResult.CONTINUE
    }

    override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
      if (pathIsNotIgnored(dir)) {
        _dirs.add(dir)
        FileVisitResult.CONTINUE
      } else {
        logger.info(s"Skipping $dir")
        FileVisitResult.SKIP_SUBTREE
      }
    }
  }

  lazy val files: Seq[Path] = _files.asScala.toSeq
  lazy val dirs: Seq[Path] = _dirs.asScala.toSeq
}

class IgnoreFileMatcher(val ignoreFileIn: Option[File]) {

  val ignoredPatterns: List[PathMatcher] = {
    ignoreFileIn.map { ignoreFile =>
      val source = Source.fromFile(ignoreFile)(Codec.UTF8)
      val cleanedLines = source.getLines().map(_.trim).filterNot(_.isEmpty).filterNot(_.startsWith("#")).toList
      val out = cleanedLines.map(pattern => FileSystems.getDefault.getPathMatcher("glob:" + pattern))
      source.close()
      out
    }.getOrElse(List.empty)
  }

  def pathIsNotIgnored(root: Path, path: Path): Boolean = {
    if (ignoreFileIn.isEmpty) {
      // file is not ignored
      true
    } else {
      val relativized = root.relativize(path)
      !ignoredPatterns.exists(_.matches(path))
    }
  }

}