package ch.descabato

import java.io.File

import ch.descabato.core_old.{FileDescription, FolderDescription, SymbolicLink, _}
import ch.descabato.utils.Implicits._
import ch.descabato.utils._
import org.scalatest.Matchers._
import org.scalatest._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

@Ignore
// TODO write a test for the new classes
class SerializationSpec extends FlatSpec with TestUtils {

  implicit def toVector[T](t: T): Vector[T] = Vector(t)

  def fixture =
    new {
	    type ChainMap = ArrayBuffer[(BytesWrapper, Array[Byte])]
	    var chainMap : ChainMap = new ChainMap()
	    chainMap += (("asdf".getBytes.wrap(),"agadfgdsfg".getBytes))
	    val f = List("README.md", "../README.md").map(new File(_)).filter(_.exists).head
      val fid = new FileDescription("test.txt", 0L, FileAttributes(f.toPath()), Hash("adsfasdfasdf".getBytes()))
      val fod = new FolderDescription("test.txt", new FileAttributes())
      val fd = new FileDeleted("asdf")
	    val symLink = new SymbolicLink("test", "asdf", new FileAttributes())
      val list : Seq[UpdatePart] = List(fid, fod, fd, symLink)
      val baout = new CustomByteArrayOutputStream()
    }
  
  def writeAndRead[T](s: Serialization, t: T)(implicit m: Manifest[T]) = {
    val f = fixture
    import f._
    s.writeObject(t, baout)
//    println("Serialization size of "+t+" with "+s.getClass().getSimpleName()+" is "+baout.size())
    val in = replay(baout)
    val t2 = s.readObject[T](in).left.get
    t2
  }
  
  def testSerializer(ser: Serialization) {
    val f = fixture
    import f._
    val fidAfter = writeAndRead(ser, fid)
    assert(fid === fidAfter)
    assert(fid.hash === fidAfter.hash)
    assert(fid.attrs.asScala.keys === fidAfter.attrs.asScala.keys)
    fidAfter.attrs.asScala.keys should contain (FileAttributes.lastModified)
    chainMap.map(_._1) should equal (writeAndRead(ser, chainMap).map(_._1))
    assert(fod === writeAndRead(ser, fod))
    assert(symLink === writeAndRead(ser, symLink))
    val listAfter = writeAndRead(ser, list)
    assert(list === writeAndRead(ser, list))
    (list.head, listAfter.head) match {
      case (x: FileDescription, y: FileDescription) => 
        assert(x.hash === y.hash)
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
