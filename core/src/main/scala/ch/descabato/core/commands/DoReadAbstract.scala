package ch.descabato.core.commands

import java.io.InputStream

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.util.ByteString
import ch.descabato.core.Universe
import ch.descabato.core.model.FileMetadataStored
import ch.descabato.frontend.{ProgressReporters, StandardByteCounter}
import ch.descabato.utils.{BytesWrapper, CompressedStream}

import scala.concurrent.Future
import scala.concurrent.duration._

abstract class DoReadAbstract(val universe: Universe) {

  import universe.{ex, materializer}

  protected val config = universe.config

  protected val readBytesCounter = new StandardByteCounter("Read Bytes")

  ProgressReporters.addCounter(readBytesCounter)
  ProgressReporters.openGui("Reading", false)

  def getInputStream(file: FileMetadataStored): InputStream = {
    val sink: Sink[ByteString, InputStream] = StreamConverters.asInputStream(10.minutes)
    val stream = getBytestream(chunkIdsForFile(file)).map(x => ByteString(x.asArray())).runWith(sink)
    stream
  }

  def chunkIdsForFile(file: FileMetadataStored): Source[Long, NotUsed] = {
    Source.fromIterator[Long](() => file.chunkIds.iterator)
  }

  def getBytestream(source: Source[Long, NotUsed]): Source[BytesWrapper, NotUsed] = {
    source.mapAsync(10) { chunkId =>
      universe.chunkStorageActor.read(chunkId)
    }.mapAsync(10) { bs =>
      Future {
        val out = CompressedStream.decompressToBytes(bs)
        readBytesCounter += out.length
        out
      }
    }
  }

}
