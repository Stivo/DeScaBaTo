package ch.descabato

import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.io.File

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
          f.listFiles().toList.foreach(walk); f.delete()
        case false => f.delete()
      }
    }
    var i = 0
    do {
      walk(f)
      i += 1
    } while (i < 5 && f.exists)
    if (i > 1) {
      l.warn(s"Took delete all $i runs, now folder is deleted "+f.exists)
    }
  }

}