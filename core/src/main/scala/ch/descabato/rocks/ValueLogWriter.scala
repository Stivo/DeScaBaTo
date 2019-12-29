package ch.descabato.rocks

import java.io.File
import java.io.InputStream
import java.io.SequenceInputStream

import ch.descabato.CompressionMode
import ch.descabato.core.util.FileReader
import ch.descabato.core.util.FileType
import ch.descabato.core.util.FileWriter
import ch.descabato.rocks.protobuf.keys.FileMetadataValue
import ch.descabato.rocks.protobuf.keys.Status
import ch.descabato.rocks.protobuf.keys.ValueLogIndex
import ch.descabato.rocks.protobuf.keys.ValueLogStatusValue
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.Hash
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging


class ValueLogWriter(rocksEnv: RocksEnv, fileType: FileType, write: Boolean = true, maxSize: Long = 512 * 1024 * 1024) extends AutoCloseable with LazyLogging {

  private val kvStore = rocksEnv.rocks
  private val config = rocksEnv.config

  private val valueLogWriteTiming = new StopWatch("writeValue")
  private val compressionTiming = new StopWatch("compression")
  private val closeTiming = new StopWatch("close")
  private val minSize = 1 * 1024 * 1024L

  //  private def init(): Int = {
  //    val value = fileType.getFiles()
  //
  //    val seq: Seq[(ValueLogStatusKey, ValueLogStatusValue)] = kvStore.getAllValueLogStatusKeys()
  //
  //    if (write) {
  //      seq.foreach { case (key, value) =>
  //        if (value.status == Status.WRITING || value.status == Status.MARKED_FOR_DELETION) {
  //          logger.warn("Deleting incomplete or obsolete file " + key)
  //          new File(folder, key.name).delete()
  //        }
  //      }
  //    }
  //    val onlyTheseFiles = seq.filter(_._1.name.startsWith(fileType))
  //    if (onlyTheseFiles.isEmpty) {
  //      0
  //    } else {
  //      onlyTheseFiles.map(_._1.parseNumber).max
  //    }
  //  }

  private var currentFile: Option[CurrentFile] = None

  def write(bytesWrapper: BytesWrapper): (ValueLogIndex, Boolean) = {
    val compressed = CompressedStream.compress(bytesWrapper, CompressionMode.snappy)
    var fileClosed = false
    val fileToWriteTo = if (currentFile.isEmpty) {
      createNewFile()
    } else {
      val file = currentFile.get
      if (file.fileWriter.currentPosition() + compressed.length > maxSize && file.fileWriter.currentPosition() >= minSize) {
        closeCurrentFile(file)
        fileClosed = true
        createNewFile()
      } else {
        file
      }
    }
    val valueLogPositionBefore = valueLogWriteTiming.measure {
      fileToWriteTo.fileWriter.write(compressed)
    }
    (ValueLogIndex(filename = fileToWriteTo.key.name, from = valueLogPositionBefore,
      lengthUncompressed = bytesWrapper.length, lengthCompressed = compressed.length), fileClosed)
  }

  private def closeCurrentFile(currentFile: CurrentFile) = {
    currentFile.fileWriter.close()
    val status = currentFile.valueLogStatusValue
    val bs = ByteString.copyFrom(currentFile.fileWriter.md5Hash().bytes)
    val newStatus = status.copy(status = Status.FINISHED, size = currentFile.fileWriter.currentPosition(), md5Hash = bs)
    kvStore.write(currentFile.key, newStatus)
  }

  private def createNewFile(): CurrentFile = {
    val file = fileType.nextFile()
    val relative = rocksEnv.config.relativePath(file)
    val key = ValueLogStatusKey(relative)
    val value = ValueLogStatusValue(status = Status.WRITING)
    kvStore.write(key, value)
    val currentFile = CurrentFile(file, key, value, config.newWriter(file))
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

  case class CurrentFile(file: File, key: ValueLogStatusKey, valueLogStatusValue: ValueLogStatusValue, fileWriter: FileWriter)

}


class ValueLogReader(rocksEnv: RocksEnv) extends AutoCloseable {

  private val readTiming = new StopWatch("readValue")
  private val decompressionTiming = new StopWatch("decompression")
  private val closeTiming = new StopWatch("close")

  private var randomAccessFiles: Map[String, FileReader] = Map.empty

  def createInputStream(fileMetadataValue: FileMetadataValue): InputStream = {

    new SequenceInputStream(new java.util.Enumeration[InputStream]() {
      val hashIterator = fileMetadataValue.hashes.toByteArray.grouped(32).iterator

      override def hasMoreElements: Boolean = hashIterator.hasNext

      override def nextElement(): InputStream = {
        val hash = hashIterator.next()
        val index = rocksEnv.rocks.readChunk(ChunkKey(Hash(hash))).get
        readValue(index).asInputStream()
      }
    })
  }

  def readValue(valueLogIndex: ValueLogIndex): BytesWrapper = {
    val file = rocksEnv.config.resolveRelativePath(valueLogIndex.filename)
    if (!randomAccessFiles.contains(valueLogIndex.filename)) {
      randomAccessFiles += valueLogIndex.filename -> rocksEnv.config.newReader(file)
    }
    val raf = randomAccessFiles(valueLogIndex.filename)
    val compressed: BytesWrapper = readTiming.measure {
      raf.readChunk(valueLogIndex.from, valueLogIndex.lengthCompressed)
    }
    decompressionTiming.measure {
      CompressedStream.decompressToBytes(compressed)
    }
  }

  override def close(): Unit = {
    closeTiming.measure {
      randomAccessFiles.values.foreach(_.close())
    }
  }
}
