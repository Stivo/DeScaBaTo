package ch.descabato

import java.io.{RandomAccessFile, File}

import ch.descabato.core.BaWrapper
import ch.descabato.core.kvstore.{EntryTypes, KvStoreReader, KvStoreWriterImpl, KvStoreReaderImpl}
import org.scalatest.ShouldMatchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FlatSpec}
import ch.descabato.utils.Utils

import scala.collection.immutable.HashMap
import scala.util.Random

class KvStoreFixingSpec extends FlatSpec with RichFlatSpecLike with BeforeAndAfter with BeforeAndAfterAll with ShouldMatchers with Utils {

  val testFile1 = new File("test1.kvs")
  val testFile2 = new File("test2.kvs")

  val maxEntries = 10000

  def makeKey(i: Int) = ("key" + i).getBytes()
  def makeValue(i: Int) = (("value"*5) + i).getBytes()

  runTests(null, "normal kvstore", testFile1)
  runTests("aksögj kdfögj ksdöfgj jkö jkö", "encrypted kvstore", testFile2)

  def runTests(passphrase: String, desc: String, testFile: File) {

    var test1Ok = true

    def partOfTest1(f: => Unit) {
      if (testFile.exists()) {
        if (!test1Ok) pending
        test1Ok = (false)
        f
        test1Ok = (true)
      }
    }

    desc should
      "write normally" in {
      testFile.delete()
      testFile.createNewFile()
      test1Ok =(true)
      partOfTest1 {
        val writer = new KvStoreWriterImpl(testFile, passphrase)
        for (i <- 1 to maxEntries) {
          writer.writeKeyValue(makeKey(i), makeValue(i))
        }
        writer.close()
      }
    }

    val r = new Random()
    for (i <- 1 to 50) {
      it should s"read normally $i" in {
        partOfTest1 {
          val reader = new KvStoreReaderImpl(testFile, passphrase)
          val entries = reader.iterator().toList
          var kvEntries = 0
          var seenEOF = false
          for (entry <- entries) {
            if (entry.typ == EntryTypes.keyValue) {
              seenEOF shouldBe (false)
              val key = new String(entry.parts.head.array).drop("key".length)
              val readValue = entry.parts.last.array
              readValue shouldBe (makeValue(key.toInt))
              kvEntries += 1
            } else {
              seenEOF = true
            }
          }
          reader.close()
          seenEOF shouldBe (true)
          l.info(s"Still has $kvEntries left")
        }
      }

      it should s"fix the entry when cutting from the back $i" in {
        partOfTest1 {
          val backup = testFile.length()
          val raf = new RandomAccessFile(testFile, "rw")
          val toCut = r.nextInt((raf.length() / 2).toInt)
          l.info(s"Cutting $toCut bytes")
          raf.setLength(raf.length() - toCut)
          raf.close()
          val reader = new KvStoreReaderImpl(testFile, passphrase, readOnly = false)
          val out = reader.checkAndFixFile()
          (out || !testFile.exists()) shouldBe (true)
          if (testFile.exists()) {
            reader.close()
            l.info(s"Length now is ${testFile.length}, cut ${backup - testFile.length - toCut} more than $toCut")
          }
        }
      }
    }

    it should "delete the file" in {
      partOfTest1 {
        val raf = new RandomAccessFile(testFile, "rw")
        raf.setLength(5)
        raf.close()
        val reader = new KvStoreReaderImpl(testFile, passphrase, readOnly = false)
        reader.checkAndFixFile()
        reader.close()
        testFile.exists() shouldBe (false)
      }
    }
  }

}
