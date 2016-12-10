package ch.descabato

import java.io.{InputStream, File}
import java.util
import java.util.{List => JList}

import ch.descabato.core.BackupFolderConfiguration
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.Implicits._
import org.apache.commons.compress.utils.IOUtils
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

  implicit class InputStreamBetter(in: InputStream) {
    def readFully() = {
      val baos = new ByteArrayOutputStream()
      IOUtils.copy(in, baos)
      baos.close()
      baos.toBytesWrapper().asArray()
    }
  }

  class FHO(passphrase: Option[String]) extends BackupFolderConfiguration(folder, "", passphrase) {
    override def toString = s"Compression mode is $compressor, keylength is $keyLength, passphrase is $passphrase"
  }

  "Compressed Streams" should "compress and decompress random bytes" in {
    forAll(minSize(0), sizeRange(1000000), minSuccessful(50)) { (compressor: CompressionMode, toEncode: Array[Byte]) => {

      val baosOriginal = new ByteArrayOutputStream()
      val toEncodeWrapped = toEncode.wrap()
      val compressed = CompressedStream.compress(toEncodeWrapped, compressor)

      val read = CompressedStream.decompressToBytes(compressed)
      assert(toEncodeWrapped equals read)
    }
    }
  }
  
  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected = true
  
}
