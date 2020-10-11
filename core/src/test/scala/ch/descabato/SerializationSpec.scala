package ch.descabato

import java.io.File

import ch.descabato.core.actors.MetadataStorageActor.BackupMetaDataStored
import ch.descabato.core.model._
import ch.descabato.utils._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks._

import scala.language.implicitConversions

// TODO write a test for the new classes
class SerializationSpec extends AnyFlatSpec with TestUtils {

  implicit def toVector[T](t: T): Vector[T] = Vector(t)

  def fixture =
    new {
      val f = List("README.md", "../README.md").map(new File(_)).filter(_.exists).head
      val baout = new CustomByteArrayOutputStream()
    }

  val f = List("README.md", "../README.md").map(new File(_)).filter(_.exists).head

  val folder = List("core", "../core").map(new File(_)).filter(_.exists).head
  val serialization = new JsonSerialization()

  val fileDescription: Gen[FileDescription] = for {
    path <- arbitrary[String]
    size <- arbitrary[Long]
    hash <- arbitrary[Array[Byte]]
  } yield FileDescription(path, size, FileAttributes(f.toPath))

  val fileMetadataStored: Gen[FileMetadataStored] = for {
    fd <- fileDescription
    id <- arbitrary[Long]
    chunkIds <- Gen.nonEmptyContainerOf[Array, Long](id)
  } yield FileMetadataStored(id, fd, chunkIds)

  val folderMetadataStored: Gen[FolderMetadataStored] = for {
    path <- arbitrary[String]
    id <- arbitrary[Long]
  } yield FolderMetadataStored(id, FolderDescription(path, FileAttributes(folder.toPath)))

  val backupMetadataStored: Gen[BackupMetaDataStored] = for {
    fileMetadatas <- Gen.containerOf[Array, FileMetadataStored](fileMetadataStored)
    folderMetadatas <- Gen.containerOf[Array, FolderMetadataStored](folderMetadataStored)
  } yield new BackupMetaDataStored(fileMetadatas, folderMetadatas)

  def writeAndRead[T](s: Serialization, t: T)(implicit m: Manifest[T]) = {
    val f = fixture
    import f._
    s.writeObject(t, baout)
//    println("Serialization size of "+t+" with "+s.getClass().getSimpleName()+" is "+baout.size())
    val in = replay(baout)
//    println("Serialized value is "+new String(baout.toBytesWrapper.asArray()))
    val t2 = s.readObject[T](in).left.get
    t2
  }

  "json" should "serialize stored chunks" in {
    forAll(minSuccessful(100)) {
      (id: Long, filename: String, hash: Array[Byte], startPos: Long, length: Long) =>
        val chunk = StoredChunk(id, filename, Hash(hash), startPos, length)
        assert(chunk === writeAndRead(serialization, chunk))
    }
  }

  it should "serialize stored file metadata" in {
    val serialization = new JsonSerialization()
    val f = List("README.md", "../README.md").map(new File(_)).filter(_.exists).head

    forAll(fileMetadataStored) { (x: FileMetadataStored) =>
      assert(x === writeAndRead(serialization, x))
    }
  }
  it should "serialize stored folder metadata" in {
    forAll(folderMetadataStored) { (x: FolderMetadataStored) =>
      assert(x === writeAndRead(serialization, x))
    }

  }
  it should "serialize stored backup metadata" in {
    forAll(backupMetadataStored) { (x: BackupMetaDataStored) =>
      assert(x === writeAndRead(serialization, x))
    }
  }

}
