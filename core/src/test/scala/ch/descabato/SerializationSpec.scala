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
import java.util.{List => JList}
import java.util.ArrayList
import scala.collection.convert.DecorateAsScala
import scala.collection.JavaConversions._
import java.io.ByteArrayInputStream

class SerializationSpec extends FlatSpec with TestUtils {
 
  def fixture =
    new {
	  type ChainMap = ArrayBuffer[(BAWrapper2, Array[Byte])]
	  var chainMap : ChainMap = new ChainMap()
	  chainMap += (("asdf".getBytes,"agadfgdsfg".getBytes))
      val f = new File("README.md")
      val fid = new FileDescription("test.txt", 0L, FileAttributes.apply(f, new MetadataOptions()))
      fid.hash = "adsfasdfasdf".getBytes()
      val fod = new FolderDescription("test.txt", FileAttributes.apply(f, new MetadataOptions()))
      fid.hash = "asdf".getBytes()
      val fd = new FileDeleted("asdf")
      val list : Seq[UpdatePart] = List(fid, fod, fd)
      val baout = new ByteArrayOutputStream()
    }
  
  def writeAndRead[T](s: Serialization, t: T)(implicit m: Manifest[T]) = {
    val f = fixture
    import f._
    
    s.writeObject(t, baout)
    println("Serialization size of "+t+" with "+s.getClass().getSimpleName()+" is "+baout.size())
    val in = replay(baout)
    val t2 = s.readObject[T](in).left.get
    t2
  }
  
  def testSerializer(ser: Serialization) {
    val f = fixture
    import f._
    val fidAfter = writeAndRead(ser, fid)
    assert(fid === fidAfter)
    println(fid.hash+ " "+ fidAfter.hash)
    assert(Arrays.equals(fid.hash, fidAfter.hash))
    assert(fid.attrs.keys === fidAfter.attrs.keys)
    fidAfter.attrs.keys should contain ("lastModifiedTime")
    chainMap.map(_._1) should equal (writeAndRead(ser, chainMap).map(_._1))
    assert(fod === writeAndRead(ser, fod))
    val listAfter = writeAndRead(ser, list)
    assert(list === writeAndRead(ser, list))
    (list.head, listAfter.head) match {
      case (x: FileDescription, y: FileDescription) => 
        assert(Arrays.equals(x.hash, y.hash))
      case _ => fail
    }
  }
  
  "json" should "serialize backubparts" in {
    val json = new JsonSerialization()
    testSerializer(json)
  }
  
 "smile" should "serialize backubparts" in {
    val smile = new SmileSerialization()
    testSerializer(smile)
  }
 
}
