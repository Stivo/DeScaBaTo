package ch.descabato.rocks

import java.nio.charset.StandardCharsets
import java.util

import better.files._
import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.core.config.BackupFolderConfiguration
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

  val Consistent = Val("Consistent")
  val Writing = Val("Writing")
  val Reconstructing = Val("Reconstructing")

  def fromBytes(value: Array[Byte]): Option[Value] = {
    Option(value).flatMap(value => values.find(v => util.Arrays.equals(v.asBytes(), value)))
  }

}

class RepairLogic(backupFolderConfiguration: BackupFolderConfiguration, readonly: Boolean) extends Utils {

  import RocksStates._

  val initialRocksEnv = RocksEnv(backupFolderConfiguration, readonly)
  val importer = new MetadataImporter(initialRocksEnv)

  val key = "rocks_status".getBytes

  def initialize(): RocksEnv = {
    (RocksStates.fromBytes(initialRocksEnv.rocks.readDefault(key)), readonly) match {
      case (None, false) =>
        // new database or unknown value, set to consistent
        // delete rocks and restart import
        logger.info("New database detected or database corrupt. Reimporting from metadata")
        closeRocks()
        deleteRocksFolder()
        reimportIfAllowed()
      case (None, true) =>
        logger.info("Database repair is recommended")
        initialRocksEnv
      case (Some(Consistent), _) =>
        // nothing to do
        initialRocksEnv
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
        initialRocksEnv
    }
  }

  def closeRocks(): Unit = {
    initialRocksEnv.close()
  }

  def deleteRocksFolder(): Unit = {
    FileUtils.deleteAll(initialRocksEnv.rocksFolder)
  }

  def reimportIfAllowed(): RocksEnv = {
    val newEnv = RocksEnv(backupFolderConfiguration, readonly)
    newEnv.rocks.writeDefault(key, RocksStates.Reconstructing.asBytes())
    newEnv.rocks.commit()
    new MetadataImporter(newEnv).importMetadata()
    newEnv.rocks.writeDefault(key, RocksStates.Consistent.asBytes())
    newEnv.rocks.commit()
    newEnv
  }

}

class MetadataExporter(rocksEnv: RocksEnv) extends Utils {
  val kvStore = rocksEnv.rocks
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


class MetadataImporter(rocksEnv: RocksEnv) extends Utils {
  val kvStore = rocksEnv.rocks
  private val filetype: StandardNumberedFileType = rocksEnv.fileManager.metadata

  def importMetadata(): Unit = {
    val restoreTime = new StandardMeasureTime()

    for (file <- filetype.getFiles()) {
      for {
        reader <- rocksEnv.config.newReader(file).autoClosed
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
      val relativePath = rocksEnv.relativize(file)
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