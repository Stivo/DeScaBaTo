package ch.descabato.core.actors

import akka.stream._
import akka.stream.stage._
import akka.util.ByteString
import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.hashes.BuzHash
import ch.descabato.utils.BytesWrapper

class Chunker(minChunkSize: Int = 128 * 1024,
              maxChunkSize: Int = 16 * 1024 * 1024,
              bits: Byte = 19) extends GraphStage[FlowShape[ByteString, BytesWrapper]] {
  val in = Inlet[ByteString]("Framer.in")
  val out = Outlet[BytesWrapper]("Framer.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    private val outputStream = new CustomByteArrayOutputStream(minChunkSize * 5)
    private val buzHash = new BuzHash(64)

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        tryPull()
      }
    })

    private def tryPull() = {
      if (!hasBeenPulled(in)) {
        pull(in)
      }
    }

    setHandler(in, new InHandler {

      private def emitNow(): Unit = {
        emit(out, outputStream.toBytesWrapper)
        outputStream.reset()
        buzHash.reset()
      }

      override def onPush(): Unit = {
        val chunk = grab(in)
        val array = chunk.toArray
        var pos = 0
        val end = array.length
        while (pos < end) {
          val remainingBytes = Math.min(maxChunkSize - outputStream.size(), array.length - pos)
          val boundaryPosOrNegative = buzHash.updateAndReportBoundary(array, pos, remainingBytes, bits)
          val readBytes = if (boundaryPosOrNegative >= 0) boundaryPosOrNegative else remainingBytes
          outputStream.write(array, pos, readBytes)
          pos += readBytes
          if (boundaryPosOrNegative >= 0) {
            if (outputStream.size() >= minChunkSize) {
              emitNow()
            }
          } else {
            if (maxChunkSize <= outputStream.size()) {
              emitNow()
            }
          }
        }
        tryPull()
      }

      override def onUpstreamFinish(): Unit = {
        emitNow()
        completeStage()
      }
    })

  }

}