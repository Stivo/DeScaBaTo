package ch.descabato.mains

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.BackupEnv
import ch.descabato.core.model.ChunkMap
import ch.descabato.core.model.FileMetadataMap
import ch.descabato.core.model.RevisionKey
import ch.descabato.core.model.ValueLogName
import ch.descabato.protobuf.keys.ProtoDb
import ch.descabato.utils.Hash
import ch.descabato.utils.StandardMeasureTime
import ch.descabato.utils.Utils

import java.io.File
import scala.util.Using

object Migration {

  def main(args: Array[String]): Unit = {
    val config = BackupFolderConfiguration(new File(args(0)), passphrase = Some(args(1)))

    val env = BackupEnv(config, true)
    val migration = new Migration(env)
    migration.migrateTo(args(2))
  }
}

class Migration(env: BackupEnv) extends Utils {

  val rethrowException: PartialFunction[Throwable, Unit] = {
    case e: Exception =>
      throw e
  }

  import env.rocks

  def migrateTo(name: String): Unit = {
    val st = new StandardMeasureTime()
    val chunkMap = ChunkMap.empty
    for (chunk <- rocks.getAllChunks()) {
      chunkMap.add(chunk._1.hash, chunk._2)
    }
    logger.info("converted chunks, took " + st.measuredTime())

    val fileMetadataMap = FileMetadataMap.empty
    for ((key, value) <- rocks.getAllFileMetadatas()) {
      val map = value.hashes.asArray().grouped(32).flatMap { x =>
        chunkMap.getByKey(Hash(x))
      }.map(_._1).filter(_ != null).toSeq
      fileMetadataMap.add(key, value.copy(hashes = null, hashIds = map))
    }
    logger.info("Converted filemetadata, took: " + st.measuredTime())

    val revisions = rocks.getAllRevisions().map { case (k, v) =>
      val fileIdentifiers = v.files.flatMap(x => fileMetadataMap.getByKey(x)).map(_._1)
      (RevisionKey(k.number), v.copy(files = Seq.empty, fileIdentifiers = fileIdentifiers))
    }

    logger.info("Converted revisions, took: " + st.measuredTime())
    val status = rocks.getAllValueLogStatusKeys().map { case (k, v) =>
      (ValueLogName(k.name), v)
    }

    val protoDb = new ProtoDb(revisions, status, chunkMap.exportForProto(), fileMetadataMap.exportForProto())
    logger.info("Converted all, took " + st.measuredTime())

    st.startMeasuring()
    Using.Manager { use =>
      val writer1 = use(env.config.newCompressedOutputStream(new File(name)))
      protoDb.writeTo(writer1)
    }.recover(rethrowException)
    logger.info("Exported map, took " + st.measuredTime() + s", size is ${Utils.readableFileSize(protoDb.serializedSize)}")

    st.startMeasuring()
    val fromProto: ProtoDb = Using.Manager { use =>
      val reader = use(env.config.newCompressedInputStream(new File(name)))
      ProtoDb.parseFrom(reader)
    }.get
    logger.info("Read map, took " + st.measuredTime() + " fromProto.chunkMap.keyMap.size " + fromProto.chunkMap.chunkKeys.size)
  }
}