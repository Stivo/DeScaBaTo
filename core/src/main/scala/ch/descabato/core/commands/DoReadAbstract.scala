package ch.descabato.core.commands

import akka.NotUsed
import akka.stream.scaladsl.Source
import ch.descabato.core.Universe
import ch.descabato.core.model.FileMetadata
import ch.descabato.core_old.BackupFolderConfiguration
import ch.descabato.utils.{BytesWrapper, CompressedStream, Hash}

import scala.concurrent.Future

abstract class DoReadAbstract(val universe: Universe) {
  import universe.ex

  protected val config = universe.config

  def hashesForFile(file: FileMetadata): Source[Array[Byte], NotUsed] = {
    Source.fromIterator[Array[Byte]](() => file.blocks.bytes.grouped(config.hashLength))
  }

  def getBytestream(source: Source[Array[Byte], NotUsed]): Source[BytesWrapper, NotUsed] = {
    source.mapAsync(10) { hashBytes =>
      val hash = Hash(hashBytes)
      universe.blockStorageActor.read(hash)
    }.mapAsync(10) { bs =>
      Future(CompressedStream.decompressToBytes(bs))
    }
  }

}