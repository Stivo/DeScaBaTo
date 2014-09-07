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
  
  implicit val x = Arbitrary {oneOf(CompressionMode.values.filter(_.isCompressionAlgorithm))}
      
  class FHO(passphrase: Option[String]) extends BackupFolderConfiguration(folder, "", passphrase) {
    override def toString = s"Compression mode is $compressor, keylength is $keyLength, passphrase is $passphrase"
  }

  "Compressed Streams" should "compress and decompress random bytes" in {
    forAll(minSize(0), maxSize(1000000), minSuccessful(50)) { (compressor: CompressionMode, toEncode: Array[Byte]) => {

      val baosOriginal = new ByteArrayOutputStream()
      val compressed = CompressedStream.compress(toEncode, compressor)

      val read = CompressedStream.decompress(compressed.toArray()).readFully
      assert(Arrays.equals(read, toEncode))
    }
    }
  }
  
  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected = true
  
  override def afterAll() {
    import ObjectPools.{foundExactCounter, foundExactRequests, foundMinimumCounter, foundMinimumRequests}
    l.info("Found minimum "+foundMinimumCounter+" / "+foundMinimumRequests)
    l.info("Found exact "+foundExactCounter+" / "+foundExactRequests)
  }

}
