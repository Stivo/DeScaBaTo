package ch.descabato

import org.scalatest._
import java.io.File
import org.scalatest.Matchers._
import java.util.Arrays
import scala.collection.mutable.HashMap
import scala.collection.mutable.Buffer
import scala.collection.mutable.Set
import scala.collection.mutable.ArrayBuffer
import java.util.{List => JList}
import java.util.ArrayList
import scala.collection.convert.DecorateAsScala
import scala.collection.JavaConversions._
import java.io.ByteArrayInputStream
import ch.descabato.core._
import ch.descabato.utils.SmileSerialization
import ch.descabato.utils.JsonSerialization
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Serialization
import ch.descabato.core.FileDescription
import ch.descabato.core.FolderDescription
import ch.descabato.core.SymbolicLink

class SerializationSpec extends FlatSpec with TestUtils {

  implicit def toBuffer[T](t: T) = Buffer(t)

  def fixture =
    new {
	  type ChainMap = ArrayBuffer[(BAWrapper2, Array[Byte])]
	  var chainMap : ChainMap = new ChainMap()
	  chainMap += (("asdf".getBytes,"agadfgdsfg".getBytes))
	  val f = List("README.md", "../README.md").map(new File(_)).filter(_.exists).head
      val fid = new FileDescription("test.txt", 0L, FileAttributes(f.toPath()))
      fid.hash = "adsfasdfasdf".getBytes()
      val fod = new FolderDescription("test.txt", new FileAttributes())
      val fd = new FileDeleted("asdf")
	  val symLink = new SymbolicLink("test", "asdf", new FileAttributes())
      val list : Seq[UpdatePart] = List(fid, fod, fd, symLink)
      val bd = new BackupDescription(fid, fod, symLink, fd)
      val baout = new ByteArrayOutputStream()
    }
  
  def writeAndRead[T](s: Serialization, t: T)(implicit m: Manifest[T]) = {
    val f = fixture
    import f._
    
    s.writeObject(t, baout)
    //println("Serialization size of "+t+" with "+s.getClass().getSimpleName()+" is "+baout.size())
    val in = replay(baout)
    val t2 = s.readObject[T](in).left.get
    t2
  }
  
  def testSerializer(ser: Serialization) {
    val f = fixture
    import f._
    val fidAfter = writeAndRead(ser, fid)
    assert(fid === fidAfter)
    assert(Arrays.equals(fid.hash, fidAfter.hash))
    assert(fid.attrs.keys === fidAfter.attrs.keys)
    fidAfter.attrs.keys should contain ("lastModifiedTime")
    chainMap.map(_._1) should equal (writeAndRead(ser, chainMap).map(_._1))
    assert(fod === writeAndRead(ser, fod))
    assert(symLink === writeAndRead(ser, symLink))
    val listAfter = writeAndRead(ser, list)
    assert(list === writeAndRead(ser, list))
    assert(bd === writeAndRead(ser, bd))
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
