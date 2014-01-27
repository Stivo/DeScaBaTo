package ch.descabato

import org.scalatest._
import java.io.File
import org.scalatest.Matchers._
import java.util.Arrays
import scala.collection.mutable.HashMap
import scala.collection.mutable.Buffer
import scala.collection.mutable.Set
import java.io.ByteArrayOutputStream
import scala.collection.mutable.ArrayBuffer
import java.util.{ List => JList }
import java.util.ArrayList
import scala.collection.convert.DecorateAsScala
import scala.collection.JavaConversions._
import java.io.ByteArrayInputStream
import java.io.InputStream
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalacheck.Gen
import scala.util.Random
import org.scalacheck.Arbitrary
import org.scalacheck._
import Arbitrary.arbitrary

class CompressedStreamSpec extends FlatSpec with BeforeAndAfterAll with GeneratorDrivenPropertyChecks with TestUtils {
  import org.scalacheck.Gen._
  
  //ConsoleManager.testSetup
//  AES.testMode = true
  
  var folder = new File("testdata/temp")
  
  
  implicit class InputStreamBetter(in: InputStream) {
    def readFully = {
      val baos = new ByteArrayOutputStream()
      Streams.copy(in, baos)
      baos.toByteArray()
    }
  }

  def randomElement[T](ar: Seq[T]) = {
    val r = new Random()
    val int = r.nextInt(ar.length - 1)
    ar(int)
  }
  
  implicit val genNode = Arbitrary {
    for {
      v <- oneOf(CompressionMode.values)
//      keyLength <- oneOf(128, 192, 256)
//      passphrase <- arbitrary[Option[String]]
    } yield {
      val out = new FHO(None);
      out.compressor = v;
//      out.keyLength = keyLength;
      out
    }
  }
  
  class FHO(passphrase: Option[String]) extends BackupFolderConfiguration(folder, "", passphrase) {
    override def toString = s"Compression mode is $compressor, keylength is $keyLength, passphrase is $passphrase"
  }
  
  forAll(minSize(0), maxSize(1000000), minSuccessful(30)) { (fho: FHO, toEncode: Array[Byte], disable: Boolean) =>
    whenever (fho.passphrase.isEmpty || fho.passphrase.get.length >= 6) {
    val baosOriginal = new ByteArrayOutputStream()
    val wrapped = CompressedStream.wrapStream(baosOriginal, fho, disable)
    wrapped.write(toEncode)
    wrapped.close()
    val (header, compressed) = CompressedStream.compress(toEncode, fho, disable)
    
    val encoded = baosOriginal.toByteArray()
    encoded(0) should be (header)
    assert(Arrays.equals(encoded.tail,compressed))
    val in = new ByteArrayInputStream(encoded)
    val read = CompressedStream.readStream(in).readFully
    assert(Arrays.equals(read, toEncode))
    }
  }

}
