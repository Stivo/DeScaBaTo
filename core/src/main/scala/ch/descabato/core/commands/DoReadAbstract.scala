package ch.descabato.core.commands

import akka.NotUsed
import akka.stream.scaladsl.Source
import ch.descabato.core.Universe
import ch.descabato.core.model.FileMetadataStored
import ch.descabato.frontend.{ProgressReporters, StandardByteCounter}
import ch.descabato.utils.{BytesWrapper, CompressedStream, Hash}

import scala.concurrent.Future

abstract class DoReadAbstract(val universe: Universe) {

  import universe.ex

  protected val config = universe.config

  protected val readBytesCounter = new StandardByteCounter("Read Bytes")

  ProgressReporters.addCounter(readBytesCounter)
  ProgressReporters.openGui("Reading", false)

  def hashesForFile(file: FileMetadataStored): Source[Array[Byte], NotUsed] = {
    Source.fromIterator[Array[Byte]](() => file.blocks.bytes.grouped(config.hashLength))
  }

  def getBytestream(source: Source[Array[Byte], NotUsed]): Source[BytesWrapper, NotUsed] = {
    source.mapAsync(10) { hashBytes =>
      val hash = Hash(hashBytes)
      universe.blockStorageActor.read(hash)
    }.mapAsync(10) { bs =>
      Future {
        val out = CompressedStream.decompressToBytes(bs)
        readBytesCounter += out.length
        out
      }
    }
  }

}
