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
 
  implicit val folder = new BackupOptions()
  
  def fixture =
    new {
	  
	  folder.backupFolder = new File("testdata/temp")
	  folder.onlyIndex = true
	  val dest = new File("test.obj")
	  dest.deleteOnExit()
	  val handler = new BackupHandler(folder)
	  val fid = handler.backupFile(new File(testdata, "empty.txt"))
	  val fod = handler.backupFolderDesc(testdata)
	  type ChainMap = ArrayBuffer[(BAWrapper2, Array[Byte])]
	  var chainMap : ChainMap = new ChainMap()
	  chainMap.add(("asdf".getBytes,"agadfgdsfg".getBytes))
    }

  
  
  def writeAndRead[T](s: Serialization, t: T)(implicit m: Manifest[T]) = {
    s.writeObject(t, new File("test.obj"))
//    println("Serialization size of "+t+" with "+s.getClass().getSimpleName()+" is "+new File("test.obj").length())
//    println(new File("test.obj").length())
    val t2 = s.readObject[T](new File("test.obj"))
    t2
  }
  
  def testWithFid(ser: Serialization, testChainMap: Boolean = true) {
    val f = fixture
    import f._
    assert(fid === writeAndRead(ser, fid))
    val fdNew = fid.copy(hash = "Testing byte serialization".getBytes)
    var fd2 = writeAndRead(ser, fdNew)
    assert(true === Arrays.equals(fdNew.hash, fd2.hash))
    assert(fod === writeAndRead(ser, fod))
    if (testChainMap)
    	assert(chainMap.map(_._1) === writeAndRead(ser, chainMap).map(_._1))
  }
  
  "kryo" should "serialize backupparts" in {
    val kryo = new KryoSerialization()
    testWithFid(kryo)
  }
  
  "json" should "serialize backubparts" in {
    val json = new JsonSerialization()
    testWithFid(json)
  }
  
 "bson" should "serialize backubparts" in {
    val bson = new BsonSerialization()
    testWithFid(bson)
  }
 
 "smile" should "serialize backubparts" in {
    val smile = new SmileSerialization()
    testWithFid(smile)
  }
 
 "xml" should "serialize backubparts, but not chainmaps" in {
    val xml = new XmlSerialization()
    testWithFid(xml, false)
  }
  
}
