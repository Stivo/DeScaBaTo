package backup

import org.scalatest._
import java.io.File
import org.scalatest.Matchers._
import java.util.Arrays
import scala.collection.mutable.HashMap

class SerializationSpec extends FlatSpec {

  ConsoleManager.testSetup
  
  val testdata = new File("testdata")
  
  implicit val folder = new BackupOptions()
  folder.backupFolder = new File("testdata/temp")
  folder.onlyIndex = true
  val dest = new File("test.obj")
  dest.deleteOnExit()
  
  val handler = new BackupHandler(folder)
  val fid = handler.backupFile(new File(testdata, "empty.txt"))
  val fod = handler.backupFolderDesc(testdata)
  var chainMap = List[(BAWrapper2, Array[Byte])]()
  chainMap ::= ("asdf".getBytes,"agadfgdsfg".getBytes)
  
  def writeAndRead[T](s: Serialization, t: T)(implicit m: Manifest[T]) = {
    s.writeObject(t, new File("test.obj"))
//    println("Serialization size of "+t+" with "+s.getClass().getSimpleName()+" is "+new File("test.obj").length())
//    println(new File("test.obj").length())
    val t2 = s.readObject[T](new File("test.obj"))
    t2
  }
  
  "kryo" should "serialize backupparts" in {
    val kryo = new KryoSerialization()
    testWithFid(kryo, fid)
  }
  
  def testWithFid(ser: Serialization, fd: FileDescription) {
    assert(fd === writeAndRead(ser, fd))
    val fdNew = fd.copy(hash = "Testing byte serialization".getBytes)
    val fd2 = writeAndRead(ser, fdNew)
    assert(true === Arrays.equals(fdNew.hash, fd2.hash))
    assert(fod === writeAndRead(ser, fod))
    assert(chainMap.map(_._1) === writeAndRead(ser, chainMap).map(_._1))
  }

  "json" should "serialize backubparts too" in {
    val json = new JsonSerialization()
    testWithFid(json, fid)
  }
  
}
