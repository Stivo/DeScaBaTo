package backup

import org.scalatest._
import java.io.File
import org.scalatest.Matchers._
import java.util.Arrays
import scala.collection.mutable.HashMap
import scala.collection.mutable.Buffer
import scala.collection.mutable.Set
import java.io.ByteArrayOutputStream
import scala.collection.mutable.ArrayBuffer
import java.util.{List => JList}
import java.util.ArrayList
import scala.collection.convert.DecorateAsScala
import scala.collection.JavaConversions._

class ZipBlockStrategySpec extends FlatSpec with BeforeAndAfterAll with BeforeAndAfter with FileDeleter {

  ConsoleManager.testSetup
  
  val testdata = new File("testdata/zipblocktest")
  
  before {
    deleteAll(testdata)
    testdata.mkdir()
    assume(testdata.exists())
    assume(testdata.listFiles().isEmpty)
  }
  
  implicit var folder : BackupOptions = null
  var totest : ZipBlockStrategy = null
  
  def fixture =
    new {
      folder = new BackupOptions()
      folder.volumeSize = Size(1024*1024)
	  folder.backupFolder = testdata
	  folder.onlyIndex = true
	  val dest = new File("test.obj")
	  dest.deleteOnExit()
      totest = new ZipBlockStrategy(folder, Some(folder.volumeSize))
      }

  "zipblock" should "save and retrieve blocks" in {
    val f = fixture
    test()
  }

  "zipblock" should "save and retrieve blocks when gzipped" in {
    val f = fixture
    folder.passphrase = "ASDF"
    folder.compression = CompressionMode.gzip
    test()
  }
  
  def test() {
    val key1 = "asfd".getBytes()
    val content = "FASDFSD".getBytes()
    
    (totest.blockExists(key1) should be (false))
    totest.writeBlock(key1, content)
    totest.finishWriting
    (totest.blockExists(key1) should be (true))
    (Arrays.equals(totest.readBlock(key1), content) should be)
    totest.free
  }

  after {
    deleteAll(testdata)
  }
  
 }
