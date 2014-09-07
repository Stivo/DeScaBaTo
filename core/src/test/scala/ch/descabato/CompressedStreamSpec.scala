package ch.descabato

import java.io.File
import java.util.Arrays
import java.util.{List => JList}

import ch.descabato.core.BackupFolderConfiguration
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.Implicits._
import org.scalacheck.Arbitrary
import org.scalatest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks

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
    import ch.descabato.utils.ObjectPools.foundExactCounter
    import ch.descabato.utils.ObjectPools.foundExactRequests
    import ch.descabato.utils.ObjectPools.foundMinimumCounter
    import ch.descabato.utils.ObjectPools.foundMinimumRequests
    l.info("Found minimum "+foundMinimumCounter+" / "+foundMinimumRequests)
    l.info("Found exact "+foundExactCounter+" / "+foundExactRequests)
  }

}
