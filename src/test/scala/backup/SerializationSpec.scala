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

class SerializationSpec extends FlatSpec with BeforeAndAfterAll {

  ConsoleManager.testSetup
  
  val testdata = new File("testdata")
  
  override def beforeAll {
    if (!testdata.exists())
      testdata.mkdirs()
    val f2 = new File(testdata, "empty.txt")
    if (!f2.exists) {
      f2.createNewFile()
    }
  }
 
  implicit var folder : BackupOptions = null
  
  def fixture =
    new {
      folder = new BackupOptions()
	  folder.backupFolder = new File("testdata/temp")
	  folder.onlyIndex = true
	  val dest = new File("test.obj")
	  dest.deleteOnExit()
	  val handler = new BackupHandler(folder)
	  val fid = handler.backupFile(new File(testdata, "empty.txt"))
	  val fod = handler.backupFolderDesc(testdata)
	  type ChainMap = ArrayBuffer[(BAWrapper2, Array[Byte])]
	  var chainMap : ChainMap = new ChainMap()
	  chainMap += (("asdf".getBytes,"agadfgdsfg".getBytes))
    }
  
  def writeAndRead[T](s: Serialization, t: T)(implicit m: Manifest[T]) = {
    s.writeObject(t, new File("test.obj"))
//    println("Serialization size of "+t+" with "+s.getClass().getSimpleName()+" is "+new File("test.obj").length())
//    println(new File("test.obj").length())
    val t2 = s.readObject[T](new File("test.obj"))
    t2
  }
  
  def testWithFid(ser: Serialization) {
    val f = fixture
    import f._
    assert(fid === writeAndRead(ser, fid))
    val fdNew = fid.copy(hash = "Testing byte serialization".getBytes)
    var fd2 = writeAndRead(ser, fdNew)
    assert(true === Arrays.equals(fdNew.hash, fd2.hash))
    assert(fod === writeAndRead(ser, fod))
    assert(chainMap.map(_._1) === writeAndRead(ser, chainMap).map(_._1))
  }
  
  "json" should "serialize backubparts" in {
    val f = fixture
    val json = new JsonSerialization()
    folder.compression = CompressionMode.none
    testWithFid(json)
  }
  
 "smile" should "serialize backubparts" in {
    val smile = new SmileSerialization()
    testWithFid(smile)
  }
 
  "smile" should "serialize backubparts with encryption" in {
    val f = fixture
    import f._
    val backup = folder.passphrase
    try {
	    folder.passphrase = "TEST"
	    val smile = new SmileSerialization()
	    val written = smile.writeObject(fid, new File("test.obj"))
	    val read = smile.readObject[FileDescription](new File("test.obj"))
	    (read should equal (fid))
	    folder.passphrase = "ASF"
	    try{
	      val read = smile.readObject[FileDescription](new File("test.obj"));
	      fail("Wrong key, still could read object")
	    } catch {
	      case _ => // expected
	    }
    } finally {
      folder.passphrase = backup
    }
  }

}
