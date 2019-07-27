package ch.descabato.core

import java.io.{File, IOException}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.{ArrayList => JArrayList}

import ch.descabato.frontend.{MaxValueCounter, StandardMaxValueCounter}

import scala.collection.JavaConverters._
import scala.io.{Codec, Source}

class FileVisitorCollector(ignoreFile: Option[File],
                           fileCounter: MaxValueCounter = new StandardMaxValueCounter("files", 0),
                           bytesCounter: MaxValueCounter = new StandardMaxValueCounter("bytes", 0)) {
  // TODO log exceptions
  private var _files: JArrayList[Path] = new JArrayList[Path]()
  private var _dirs: JArrayList[Path] = new JArrayList[Path]()

  val ignoredPatterns: Iterable[PathMatcher] = {
    ignoreFile.map { file =>
      val source = Source.fromFile(file)(Codec.UTF8)
      val cleanedLines = source.getLines().map(_.trim).filterNot(_.isEmpty).filterNot(_.startsWith("#")).toList
      cleanedLines.map(pattern => FileSystems.getDefault().getPathMatcher("glob:"+pattern))
    }.getOrElse(Nil)
  }

  def walk(path: Path): Unit = {
    val visitor = new FileVisitor(path)
    Files.walkFileTree(path, visitor)
  }

  class FileVisitor(root: Path) extends SimpleFileVisitor[Path] {

    def pathIsNotIgnored(path: Path): Boolean = {
      for(matcher <- ignoredPatterns) {
        if (matcher.matches(root.relativize(path))) {
          return false
        }
      }
      true
    }

    override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
      FileVisitResult.CONTINUE
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
      FileVisitResult.CONTINUE
    }

    override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
      if (pathIsNotIgnored(dir)) {
        _dirs.add(dir)
        FileVisitResult.CONTINUE
      } else {
        FileVisitResult.SKIP_SUBTREE
      }
    }
  }

  lazy val files: Seq[Path] = _files.asScala.toSeq
  lazy val dirs: Seq[Path] = _dirs.asScala.toSeq
}
