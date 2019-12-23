package ch.descabato.rocks

import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileOutputStream
import java.io.InputStream
import java.io.RandomAccessFile
import java.io.SequenceInputStream

import ch.descabato.rocks.protobuf.keys.FileMetadataValue
import ch.descabato.rocks.protobuf.keys.Status
import ch.descabato.rocks.protobuf.keys.ValueLogIndex
import ch.descabato.rocks.protobuf.keys.ValueLogStatusValue
import com.github.luben.zstd.Zstd
import com.typesafe.scalalogging.LazyLogging


class ValueLogWriter(folder: File, kvStore: RocksDbKeyValueStore, write: Boolean, maxSize: Long = 512 * 1024 * 1024L) extends AutoCloseable with LazyLogging {

  if (!folder.exists()) {
    logger.info(s"Creating folder for for valuelogs at $folder")
    folder.mkdir()
  }
  private val valueLogWriteTiming = new StopWatch("writeValue")
  private val compressionTiming = new StopWatch("compression")
  private val closeTiming = new StopWatch("close")
  private val minSize = 1 * 1024 * 1024L

  private def init(): Int = {
    val seq: Seq[(ValueLogStatusKey, ValueLogStatusValue)] = kvStore.getAllValueLogStatusKeys()
    if (write) {
      seq.foreach { case (key, value) =>
        if (value.status == Status.WRITING || value.status == Status.MARKED_FOR_DELETION) {
          logger.warn("Deleting incomplete or obsolete file " + key)
          new File(folder, key.name).delete()
        }
      }
    }
    if (seq.isEmpty) {
      0
    } else {
      seq.map(_._1.parseNumber).max
    }
  }

  private var highestNumber: Int = init()

  private var currentFile: Option[CurrentFile] = None

  def write(toByteArray: Array[Byte]): (ValueLogIndex, Boolean) = {
    val compressed = compressionTiming.measure {
      Zstd.compress(toByteArray)
    }
    var fileClosed = false
    val fileToWriteTo = if (currentFile.isEmpty) {
      createNewFile()
    } else {
      val file = currentFile.get
      if (file.position + compressed.length > maxSize && file.position >= minSize) {
        closeCurrentFile(file)
        fileClosed = true
        createNewFile()
      } else {
        file
      }
    }
    val valueLogPositionBefore = fileToWriteTo.position
    valueLogWriteTiming.measure {
      fileToWriteTo.fileOutputStream.write(compressed)
    }
    fileToWriteTo.position += compressed.length
    (ValueLogIndex(filename = fileToWriteTo.key.name, from = valueLogPositionBefore, lengthUncompressed = toByteArray.length, lengthCompressed = compressed.length), fileClosed)
  }

  private def closeCurrentFile(currentFile: CurrentFile) = {
    currentFile.fileOutputStream.close()
    highestNumber += 1
    val status = currentFile.valueLogStatusValue
    val newStatus = status.copy(status = Status.FINISHED, size = currentFile.position)
    kvStore.write(currentFile.key, newStatus)
  }

  private def createNewFile(): CurrentFile = {
    val filename = "values_" + (highestNumber + 1) + ".log"
    val key = ValueLogStatusKey(filename)
    val value = ValueLogStatusValue(status = Status.WRITING)
    val currentFile = CurrentFile(key, value, new FileOutputStream(new File(folder, filename)))
    this.currentFile = Some(currentFile)
    currentFile
  }

  override def close(): Unit = {
    closeTiming.measure {
      currentFile.foreach { file =>
        closeCurrentFile(file)
      }
    }
  }

  case class CurrentFile(key: ValueLogStatusKey, valueLogStatusValue: ValueLogStatusValue, fileOutputStream: FileOutputStream) {
    var position = 0L
  }

}


class ValueLogReader(rocksEnv: RocksEnv) extends AutoCloseable {

  private val readTiming = new StopWatch("readValue")
  private val decompressionTiming = new StopWatch("decompression")
  private val closeTiming = new StopWatch("close")

  private var randomAccessFiles: Map[String, RandomAccessFile] = Map.empty

  def createInputStream(fileMetadataValue: FileMetadataValue): InputStream = {

    new SequenceInputStream(new java.util.Enumeration[InputStream]() {
      val hashIterator = fileMetadataValue.hashes.toByteArray.grouped(32).iterator

      override def hasMoreElements: Boolean = hashIterator.hasNext

      override def nextElement(): InputStream = {
        val hash = hashIterator.next()
        val index = rocksEnv.rocks.readChunk(ChunkKey(hash)).get
        new ByteArrayInputStream(readValue(index))
      }
    })
  }

  def readValue(valueLogIndex: ValueLogIndex): Array[Byte] = {
    val file = new File(rocksEnv.valuelogsFolder, valueLogIndex.filename)
    if (!randomAccessFiles.contains(valueLogIndex.filename)) {
      randomAccessFiles += valueLogIndex.filename -> new RandomAccessFile(file, "r")
    }
    val raf = randomAccessFiles(valueLogIndex.filename)
    val compressed = Array.ofDim[Byte](valueLogIndex.lengthCompressed)
    readTiming.measure {
      raf.seek(valueLogIndex.from)
      raf.read(compressed)
    }
    decompressionTiming.measure {
      Zstd.decompress(compressed, valueLogIndex.lengthUncompressed)
    }
  }

  override def close(): Unit = {
    closeTiming.measure {
      randomAccessFiles.values.foreach(_.close())
    }
  }
}
