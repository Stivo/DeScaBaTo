package ch.descabato

import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream

trait TestUtils {
  
  def replay(out: ByteArrayOutputStream) = {
    new ByteArrayInputStream(finishByteArrayOutputStream(out))
  }
  
  def finishByteArrayOutputStream(out: ByteArrayOutputStream) = {
    out.close()
	out.toByteArray()
  }



}