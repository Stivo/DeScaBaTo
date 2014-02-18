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
import utils.Implicits._
import ch.descabato.core.BackupFolderConfiguration
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.ObjectPools
import ch.descabato.utils.Streams
import ch.descabato.utils.Utils
import ch.descabato.core.Block

class CompressedStreamSpec extends FlatSpec with BeforeAndAfterAll 
	with GeneratorDrivenPropertyChecks with TestUtils {
  import org.scalacheck.Gen._
  
  //ConsoleManager.testSetup
//  AES.testMode = true
  
  var folder = new File("testdata/temp")
  
  implicit val x = Arbitrary {oneOf(CompressionMode.values)}
      
  class FHO(passphrase: Option[String]) extends BackupFolderConfiguration(folder, "", passphrase) {
    override def toString = s"Compression mode is $compressor, keylength is $keyLength, passphrase is $passphrase"
  }
  
  forAll(minSize(0), maxSize(1000000), minSuccessful(50)) 
  		{ (compressor: CompressionMode, toEncode: Array[Byte]) => {
    val baosOriginal = new ByteArrayOutputStream()
    val wrapped = CompressedStream.wrapStream(baosOriginal, compressor)
    wrapped.write(toEncode)
    wrapped.close()
    
    val (header, compressed) = CompressedStream.compress(toEncode, compressor) 
    
    val encoded = baosOriginal.toByteArray(true)
    encoded(0) should be (header)
    // LZMA and Bzip2 can set smaller dictionary size if the input size is known
    if (CompressionMode.lzma != compressor && CompressionMode.bzip2 != compressor)
      assert(Arrays.equals(encoded.tail,compressed.toArray()))
    else {
      l.info(compressor+": "+encoded.tail.length+" without dict size set and "+compressed.remaining()+" with dict size set")
      val in = new ByteArrayInputStream(Array(header)++compressed.toArray())
      val read = CompressedStream.readStream(in).readFully
      assert(Arrays.equals(read, toEncode))
    }
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
