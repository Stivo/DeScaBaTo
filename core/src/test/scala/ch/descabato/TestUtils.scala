package ch.descabato

import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.io.File
import java.nio.file.Files
import org.scalatest.FreeSpec
import org.scalatest.FlatSpec

trait TestUtils extends Utils {

  def replay(out: ByteArrayOutputStream) = {
    new ByteArrayInputStream(finishByteArrayOutputStream(out))
  }

  def finishByteArrayOutputStream(out: ByteArrayOutputStream) = {
    out.close()
    out.toByteArray()
  }

  def deleteAll(f: File) = {
    def walk(f: File) {
      f.isDirectory() match {
        case true =>
          f.listFiles().toList.foreach(walk); 
          f.delete()
        case false => 
          f.delete()
          Files.deleteIfExists(f.toPath())
      }
    }
    var i = 0
    do {
      walk(f)
      i += 1
      Thread.sleep(500)
    } while (i < 5 && f.exists)
    if (i > 1) {
      l.warn(s"Took delete all $i runs, now folder is deleted "+(!f.exists))
    }
  }

}

/**
 * Adds a field `currentTestName` that you can use inside a FreeSpec test,
 * if you for example have many tests that take rather long, and you wonder
 * which one is currently running.
 */
class RichFlatSpec extends FlatSpec {

  private var _currentTestName: Option[String] = None
  def currentTestName = _currentTestName getOrElse "DwE90RXP2"

  protected override def runTest(testName: String, args: org.scalatest.Args) = {
    _currentTestName = Some(testName)
    super.runTest(testName, args)
  }
}