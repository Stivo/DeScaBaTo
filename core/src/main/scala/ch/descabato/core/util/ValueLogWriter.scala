package ch.descabato.core.util

import ch.descabato.core.model.BackupEnv
import ch.descabato.core.model.ValueLogStatusKey
import ch.descabato.frontend.ProgressReporters
import ch.descabato.frontend.StandardByteCounter
import ch.descabato.frontend.StandardCounter
import ch.descabato.frontend.StandardMaxValueCounter
import ch.descabato.protobuf.keys.FileMetadataValue
import ch.descabato.protobuf.keys.Status
import ch.descabato.protobuf.keys.ValueLogIndex
import ch.descabato.protobuf.keys.ValueLogStatusValue
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.CompressedBytes
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.StopWatch
import com.typesafe.scalalogging.LazyLogging

import java.io.File
import java.io.FileNotFoundException
import java.io.InputStream
import java.io.SequenceInputStream

class ValueLogWriter(backupEnv: BackupEnv, fileType: StandardNumberedFileType, write: Boolean = true, maxSize: Long = 512 * 1024 * 1024) extends AutoCloseable with LazyLogging {

  val bytesWrittenCounter = new StandardByteCounter("Bytes written")
  val bytesCoveredCounter = new StandardByteCounter("Bytes covered")
  val filesWrittenCounter = new StandardCounter("New volumes written")
  val compressionCounter = new StandardCounter("New volumes written")

  private val compressionRatioCounter = new StandardMaxValueCounter("Compression Ratio", 100) {
    override def update(): Unit = {
      if (bytesCoveredCounter.current == 0) {
        current = 0
      } else {
        current = bytesWrittenCounter.current * 100 / bytesCoveredCounter.current
      }
    }

    override def formatted: String = s"${super.formatted}, wrote: ${bytesWrittenCounter.formatted} in ${filesWrittenCounter.current} files"
  }

  private val usedIdentifiers: Set[Int] = {
    backupEnv.rocks.getAllValueLogStatusKeys()
      .map(key => backupEnv.config.resolveRelativePath(key._1.name))
      .filter(fileType.matches)
      .map(fileType.numberOfFile)
      .toSet
  }

  private val kvStore = backupEnv.rocks
  private val config = backupEnv.config

  private val valueLogWriteTiming = new StopWatch("writeValue")
  private val compressionTiming = new StopWatch("compression")
  private val closeTiming = new StopWatch("close")
  private val minSize = 1 * 1024 * 1024L

  private var currentFile: Option[CurrentFile] = None

  def write(compressedBytes: CompressedBytes): (Boolean, ValueLogIndex) = {
    var isNewFile = false
    val fileToWriteTo = if (currentFile.isEmpty) {
      createNewFile()
    } else {
      val file = currentFile.get
      if (file.fileWriter.currentPosition() + compressedBytes.compressedLength > maxSize && file.fileWriter.currentPosition() >= minSize) {
        isNewFile = true
        closeCurrentFile(file)
        createNewFile()
      } else {
        file
      }
    }
    val valueLogPositionBefore = valueLogWriteTiming.measure {
      fileToWriteTo.fileWriter.write(compressedBytes.compressed)
    }
    bytesWrittenCounter += compressedBytes.compressedLength
    bytesCoveredCounter += compressedBytes.uncompressedLength
    (isNewFile, ValueLogIndex(filename = fileToWriteTo.key.name, from = valueLogPositionBefore,
      lengthUncompressed = compressedBytes.uncompressedLength, lengthCompressed = compressedBytes.compressedLength))
  }

  private def closeCurrentFile(currentFile: CurrentFile): Unit = {
    currentFile.fileWriter.close()
    val status = currentFile.valueLogStatusValue
    val bs = currentFile.fileWriter.md5Hash().wrap()
    val newStatus = status.copy(status = Status.FINISHED, size = currentFile.fileWriter.currentPosition(), md5Hash = bs)
    kvStore.writeStatus(currentFile.key, newStatus)
    logger.info(s"Renaming ${currentFile.file} to final name")
    fileType.renameTempFileToFinal(currentFile.file)
  }

  private def createNewFile(): CurrentFile = {
    ProgressReporters.addCounter(compressionRatioCounter)
    filesWrittenCounter += 1
    val file = fileType.nextFile(temp = true, usedIdentifiers)
    val number = fileType.numberOfFile(file)
    val finalFile = fileType.fileForNumber(number, temp = false)
    val relative = backupEnv.config.relativePath(finalFile)
    val key = ValueLogStatusKey(relative)
    val value = ValueLogStatusValue(status = Status.WRITING)
    logger.info(s"From now on data will be written to ${key.name}")
    kvStore.writeStatus(key, value)
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


class ValueLogReader(backupEnv: BackupEnv) extends AutoCloseable with LazyLogging {

  private val readTiming = new StopWatch("readValue")
  private val decompressionTiming = new StopWatch("decompression")
  private val closeTiming = new StopWatch("close")

  private var randomAccessFiles: Map[String, FileReader] = Map.empty

  def createInputStream(fileMetadataValue: FileMetadataValue): InputStream = {

    new SequenceInputStream(new java.util.Enumeration[InputStream]() {
      private val indexIterator = backupEnv.rocks.getIndexes(fileMetadataValue)

      override def hasMoreElements: Boolean = indexIterator.hasNext

      override def nextElement(): InputStream = {
        val index = indexIterator.next()
        readValue(index).asInputStream()
      }
    })
  }

  def assertChunkIsCovered(c: ValueLogIndex): Unit = {
    val raf = lookupRaf(c)
    if (raf.file.length() < c.from + c.lengthCompressed) {
      throw new FileNotFoundException(s"File is too short to cover $c")
    }
  }

  def readValue(valueLogIndex: ValueLogIndex): BytesWrapper = {
    try {
      val raf: FileReader = lookupRaf(valueLogIndex)
      val compressed: BytesWrapper = readTiming.measure {
        raf.readChunk(valueLogIndex.from, valueLogIndex.lengthCompressed)
      }
      decompressionTiming.measure {
        CompressedStream.decompressToBytes(compressed)
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to read value from $valueLogIndex", e)
        throw e
    }
  }

  private def lookupRaf(valueLogIndex: ValueLogIndex) = {
    val file = backupEnv.config.resolveRelativePath(valueLogIndex.filename)
    if (!randomAccessFiles.contains(valueLogIndex.filename)) {
      randomAccessFiles += valueLogIndex.filename -> backupEnv.config.newReader(file)
    }
    randomAccessFiles(valueLogIndex.filename)
  }

  override def close(): Unit = {
    closeTiming.measure {
      randomAccessFiles.values.foreach(_.close())
    }
  }
}
