package ch.descabato

import java.io.ByteArrayInputStream
import java.io.File
import java.nio.file.Files

import ch.descabato.utils.Utils
import org.scalatest.flatspec.AnyFlatSpecLike

trait TestUtils extends Utils {

  def replay(out: CustomByteArrayOutputStream) = {
    new ByteArrayInputStream(finishByteArrayOutputStream(out))
  }

  def finishByteArrayOutputStream(out: CustomByteArrayOutputStream) = {
    out.close()
    out.toBytesWrapper.asArray()
  }

  // TODO there is a copy of this now in FileUtils
  def deleteAll(folders: File*): Unit = {
    def walk(f: File): Unit = {
      f.isDirectory() match {
        case true =>
          f.listFiles().toList.foreach(walk)
          f.delete()
        case false =>
          f.delete()
          Files.deleteIfExists(f.toPath())
      }
    }

    for (f <- folders) {
      var i = 0
      while ( {
        {
          walk(f)
          i += 1
          Thread.sleep(500)
        }
        i < 5 && f.exists
      })
        if (i > 1) {
          l.warn(s"Took delete all $i runs, now folder is deleted " + (!f.exists))
        }
    }
  }

}

/**
 * Adds a field `currentTestName` that you can use inside a FlatSpecLike test,
 * if you for example have many tests that take rather long, and you wonder
 * which one is currently running.
 */
trait RichFlatSpecLike extends AnyFlatSpecLike {

  private var _currentTestName: Option[String] = None

  def currentTestName = _currentTestName getOrElse "DwE90RXP2"

  protected override def runTest(testName: String, args: org.scalatest.Args) = {
    _currentTestName = Some(testName)
    super.runTest(testName, args)
  }
}