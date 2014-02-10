package ch.descabato

import org.scalatest._
import java.io.File
import org.scalatest.Matchers._
import java.util.Arrays
import scala.collection.mutable.HashMap
import scala.collection.mutable.Buffer
import scala.collection.mutable.Set
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
import utils.Utils.ByteBufferUtils
import ch.descabato.core.BackupFolderConfiguration
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.ObjectPools
import ch.descabato.utils.Streams
import ch.descabato.utils.Utils

class CompressedStreamSpec extends FlatSpec with BeforeAndAfterAll 
	with GeneratorDrivenPropertyChecks with TestUtils {
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

  implicit val x = Arbitrary {oneOf(CompressionMode.values)}
      
  class FHO(passphrase: Option[String]) extends BackupFolderConfiguration(folder, "", passphrase) {
    override def toString = s"Compression mode is $compressor, keylength is $keyLength, passphrase is $passphrase"
  }
  
  forAll(minSize(0), maxSize(1000000), minSuccessful(50)) 
  		{ (compressor: CompressionMode, toEncode: Array[Byte], disable: Boolean) => {
    val baosOriginal = new ByteArrayOutputStream()
    val wrapped = CompressedStream.wrapStream(baosOriginal, compressor, disable)
    wrapped.write(toEncode)
    wrapped.close()
    val (header, compressed) = CompressedStream.compress(toEncode, compressor, disable)
    
    val encoded = baosOriginal.toByteArray(true)
    encoded(0) should be (header)
    if (CompressionMode.lzma != compressor) // LZMA can use a small dictionary when it knows the input size in advance.
      assert(Arrays.equals(encoded.tail,compressed.toArray()))
    val in = new ByteArrayInputStream(encoded)
    val read = CompressedStream.readStream(in).readFully
    assert(Arrays.equals(read, toEncode))
    }
  }
  
  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected = true
  
  override def afterAll() {
    import ObjectPools.{foundExactCounter, foundExactRequests, foundMinimumCounter, foundMinimumRequests}
    l.info("Found minimum "+foundMinimumCounter+" / "+foundMinimumRequests)
    l.info("Found exact "+foundExactCounter+" / "+foundExactRequests)
  }

}
