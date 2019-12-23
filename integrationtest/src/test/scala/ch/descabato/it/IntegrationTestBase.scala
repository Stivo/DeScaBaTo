package ch.descabato.it

import java.io.File
import java.io.RandomAccessFile
import java.nio.file.Files
import java.nio.file.Paths

import ch.descabato.utils.Utils
import ch.descabato.RichFlatSpecLike
import ch.descabato.TestUtils
import org.apache.commons.exec.CommandLine
import org.apache.commons.exec.ExecuteException
import org.apache.commons.io.{FileUtils => IO_FileUtils}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import scala.collection.JavaConverters._
import scala.collection.mutable.Set

abstract class IntegrationTestBase extends FlatSpec with RichFlatSpecLike with TestUtils {

  def mainClass: String

  val suffix = if (Utils.isWindows) ".bat" else ""

  val descabatoFolder = {
    val folder = Paths.get("").toAbsolutePath().toFile()
    if (folder.listFiles().map(_.getName()).contains("core")) {
      folder
    } else {
      folder.getParentFile()
    }
  }

  val batchfile = new File(descabatoFolder, s"core/target/pack/bin/descabato$suffix").getAbsoluteFile()
  val packFolder = new File(descabatoFolder, s"core/target/pack").getAbsoluteFile()

  // var baseFolder = new File("L:/testdata")
  var baseFolder = {
    new File("integrationtest/testdata")
  }

  var logFolder = new File(descabatoFolder, "integrationtest/logs")

  def folder(s: String) = new File(baseFolder, s).getAbsoluteFile()

  def createHandler(args: Seq[String], redirect: Boolean = true, maxSeconds: Int = 600) = {
    if (Utils.isWindows)
      createHandlerJava(args, redirect, maxSeconds)
    else
      createHandlerScript(args, redirect, maxSeconds)
  }

  def createHandlerJava(args: Seq[String], redirect: Boolean = true, maxSeconds: Int = 600) = {
    val friendlyTestName = currentTestName.replace("/", "_")
    val cmdLine = new CommandLine("java")
    val libFolder = new File(packFolder, "lib")
    cmdLine.addArgument(s"-cp")
    cmdLine.addArgument(s"$libFolder/*")
    cmdLine.addArgument(mainClass)
    cmdLine.addArgument(args.head)
    cmdLine.addArgument("--logfile")
    cmdLine.addArgument(friendlyTestName + ".log")
    cmdLine.addArgument("--no-gui")
    args.tail.foreach { arg =>
      cmdLine.addArgument(arg)
    }
    new BackupExecutionHandler(cmdLine, packFolder, friendlyTestName, maxSeconds)
  }

  def createHandlerScript(args: Seq[String], redirect: Boolean = true, maxSeconds: Int = 600) = {
    val friendlyTestName = currentTestName.replace("/", "_")
    val cmdLine = new CommandLine(batchfile)
    cmdLine.addArgument(args.head)
    cmdLine.addArgument("--logfile")
    cmdLine.addArgument(friendlyTestName + ".log")
    cmdLine.addArgument("--no-gui")
    args.tail.foreach { arg =>
      cmdLine.addArgument(arg)
    }
    new BackupExecutionHandler(cmdLine, packFolder, friendlyTestName, maxSeconds)
  }

  def startAndWait(args: Seq[String], redirect: Boolean = true) = {
    try {
      createHandler(args, redirect).startAndWait
    } catch {
      case e: ExecuteException => e.getExitValue()
    }
  }

  def messupBackupFiles(folder: File) {
    val files = Files.walk(folder.toPath).iterator().asScala.map(_.toFile).filter(_.isFile).toList
    val set = Set("hashlists", "files")
    val prefix = if (currentTestName contains "prefix") "testprefix_" else ""
    files.filter(x => set.exists(s => x.getName.toLowerCase().startsWith(prefix + s))).foreach { f =>
      l.info("Messing up " + f + " length " + f.length())
      val raf = new RandomAccessFile(f, "rw")
      raf.seek(raf.length() / 2)
      raf.write(("\u0000").getBytes)
      raf.close()
    }
    files.filter(_.getName.startsWith(prefix + "volume")).filter(_.length > 100 * 1024).foreach { f =>
      l.info("Messing up " + f)
      val raf = new RandomAccessFile(f, "rw")
      raf.seek(raf.length() / 2)
      raf.write(("\u0000" * 100).getBytes)
      raf.close()
    }
  }

  def getFile(s1: Iterable[File], file: File) = {
    val option = s1.find(_.getName() == file.getName())
    assert(option.isDefined, s"Did not find file $file")
    option.get
  }

  def compareBackups(f1: File, f2: File) {
    val files1 = f1.listFiles().toSet
    val files2 = f2.listFiles().toSet
    for (c1 <- files1) {
      val c2 = getFile(files2, c1)
      if (c1.isDirectory()) {
        c2 should be('directory)
        compareBackups(c1, c2)
      } else {
        assert(c1.getName === c2.getName, "name should be same for " + c1)
        assert(c1.length === c2.length, "length should be same for " + c1)
        assert(c1.lastModified === c2.lastModified +- 1000, "Last modified should be within a second for " + c1)
        assert(IO_FileUtils.contentEquals(c1, c2), "content should be same for " + c1)
      }
    }
  }

}
