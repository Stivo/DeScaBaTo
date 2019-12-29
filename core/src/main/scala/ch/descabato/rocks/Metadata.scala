package ch.descabato.rocks

import java.nio.charset.StandardCharsets
import java.util

import better.files._
import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.core.util.StandardNumberedFileType
import ch.descabato.rocks.protobuf.keys.Status
import ch.descabato.rocks.protobuf.keys.ValueLogStatusValue
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.FileUtils
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Utils
import com.google.protobuf.ByteString

object RocksStates extends Enumeration {

  protected case class Val(name: String) extends super.Val {
    def asBytes(): Array[Byte] = {
      super.toString().getBytes(StandardCharsets.UTF_8)
    }
  }

  import scala.language.implicitConversions

  implicit def valueToRocksStateVal(x: Value): Val = x.asInstanceOf[Val]

  val Consistent: Val = Val("Consistent")
  val Writing: Val = Val("Writing")
  val Reconstructing: Val = Val("Reconstructing")

  def fromBytes(value: Array[Byte]): Option[Value] = {
    Option(value).flatMap(value => values.find(v => util.Arrays.equals(v.asBytes(), value)))
  }

}

class RepairLogic(rocksEnvInit: RocksEnvInit) extends Utils {

  import RocksStates._

  val initialRocks: RocksDbKeyValueStore = RocksDbKeyValueStore(rocksEnvInit)

  val importer = new MetadataImporter(rocksEnvInit, initialRocks)

  val key: Array[Byte] = "rocks_status".getBytes

  def initialize(): RocksDbKeyValueStore = {
    (RocksStates.fromBytes(initialRocks.readDefault(key)), rocksEnvInit.readOnly) match {
      case (None, false) =>
        // new database or unknown value, set to consistent
        // delete rocks and restart import
        logger.info("New database detected or database corrupt. Reimporting from metadata")
        closeRocks()
        deleteRocksFolder()
        reimportIfAllowed()
      case (None, true) =>
        logger.info("Database repair is recommended")
        initialRocks
      case (Some(Consistent), _) =>
        // nothing to do
        initialRocks
      case (Some(Writing), _) =>
        // start repair
        // TODO
        ???
      case (Some(Reconstructing), false) =>
        // delete rocks and restart import
        logger.info("Reconstruction was interrupted last time. Beginning Reconstruction again.")
        closeRocks()
        deleteRocksFolder()
        reimportIfAllowed()
      case (Some(Reconstructing), true) =>
        // delete rocks and restart import
        logger.info("Reconstruction was interrupted last time. Repair is recommended")
        initialRocks
    }
  }

  def closeRocks(): Unit = {
    initialRocks.close()
  }

  def deleteRocksFolder(): Unit = {
    FileUtils.deleteAll(rocksEnvInit.rocksFolder)
  }

  def reimportIfAllowed(): RocksDbKeyValueStore = {
    val rocks = RocksDbKeyValueStore(rocksEnvInit)
    rocks.writeDefault(key, RocksStates.Reconstructing.asBytes())
    rocks.commit()
    new MetadataImporter(rocksEnvInit, rocks).importMetadata()
    rocks.writeDefault(key, RocksStates.Consistent.asBytes())
    rocks.commit()
    rocks
  }

}

class MetadataExporter(rocksEnv: RocksEnv) extends Utils {
  val kvStore: RocksDbKeyValueStore = rocksEnv.rocks
  private val filetype: StandardNumberedFileType = rocksEnv.fileManager.metadata

  def exportUpdates(): Unit = {
    val backupTime = new StandardMeasureTime()

    val contentValues = kvStore.readAllUpdates()

    val baos = new CustomByteArrayOutputStream()
    contentValues.foreach { case (_, value) =>
      baos.write(value.asArray())
    }
    for (valueLog <- new ValueLogWriter(rocksEnv, filetype, write = true, rocksEnv.config.volumeSize.bytes).autoClosed) {
      valueLog.write(baos.toBytesWrapper)
    }
    contentValues.foreach { case (key, _) =>
      kvStore.delete(key, writeAsUpdate = false)
    }
    logger.info(s"Finished exporting updates, took " + backupTime.measuredTime())
    kvStore.commit()
  }
}


class MetadataImporter(rocksEnvInit: RocksEnvInit, kvStore: RocksDbKeyValueStore) extends Utils {

  private val filetype: StandardNumberedFileType = rocksEnvInit.fileManager.metadata

  def importMetadata(): Unit = {
    val restoreTime = new StandardMeasureTime()

    for (file <- filetype.getFiles()) {
      for {
        reader <- rocksEnvInit.config.newReader(file).autoClosed
        decompressed <- CompressedStream.decompressToBytes(reader.readAllContent()).asInputStream().autoClosed
        encodedValueOption <- LazyList.continually(RevisionContentValue.readNextEntry(decompressed)).takeWhile(_.isDefined)
        encodedValue <- encodedValueOption
      } {
        val (deletion, key, value) = kvStore.decodeRevisionContentValue(encodedValue)
        if (deletion) {
          kvStore.delete(key, writeAsUpdate = false)
        } else {
          kvStore.write(key, value.get, writeAsUpdate = false)
        }
      }
      kvStore.commit()
    }
    for (file <- filetype.getFiles()) {
      val relativePath = rocksEnvInit.config.relativePath(file)
      val key = ValueLogStatusKey(relativePath)
      val status = kvStore.readValueLogStatus(key)
      if (status.isEmpty) {
        logger.info(s"Adding status = finished for $relativePath")
        kvStore.write(key, ValueLogStatusValue(Status.FINISHED, file.length(), ByteString.copyFrom(file.toScala.digest("MD5"))))
      }
    }
    logger.info(s"Finished importing metadata, took " + restoreTime.measuredTime())
    kvStore.commit()
  }
}