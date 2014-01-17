package ch.descabato

import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.io.File

trait TestUtils {

  def replay(out: ByteArrayOutputStream) = {
    new ByteArrayInputStream(finishByteArrayOutputStream(out))
  }

  def finishByteArrayOutputStream(out: ByteArrayOutputStream) = {
    out.close()
    out.toByteArray()
  }

  def deleteAll(f: File) {
    def walk(f: File) {
      f.isDirectory() match {
        case true =>
          f.listFiles().foreach(walk); f.delete()
        case false => f.delete()
      }
    }
    walk(f)
  }

}