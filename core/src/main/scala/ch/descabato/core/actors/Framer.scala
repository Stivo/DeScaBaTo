package ch.descabato.core.actors

import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString
import ch.descabato.hashes.BuzHash
import ch.descabato.utils.BytesWrapper

class Framer(prefix: String = "") extends GraphStage[FlowShape[ByteString, BytesWrapper]] {
  val in = Inlet[ByteString]("Framer.in")
  val out = Outlet[BytesWrapper]("Framer.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    private var byteString = ByteString.empty
    private val buzHash = new BuzHash(64)

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        if (!hasBeenPulled(in)) {
          pull(in)
        }
      }
    })

    setHandler(in, new InHandler {

      def emitBytesWrapper(byteString: ByteString) = {
        emit(out, BytesWrapper(byteString.toArray))
      }

      override def onPush(): Unit = {
        val chunk = grab(in)
        val array = chunk.toArray
        var pos = 0
        val end = array.length
        while (pos < end) {
          val i = buzHash.updateAndReportBoundary(array, pos, array.length - pos, 20)
          if (i > 0) {
            byteString = byteString ++ chunk.slice(pos, pos + i)
            pos += i
            //println(s"${prefix} Emitting ${byteString.size}")
            emitBytesWrapper(byteString)
            byteString = ByteString.empty
            buzHash.reset()
          } else {
            byteString = byteString ++ chunk.slice(pos, end - pos)
            pos = end
          }
        }
        if (!hasBeenPulled(in)) {
          pull(in)
        }
      }

      override def onUpstreamFinish(): Unit = {
        emitBytesWrapper(byteString)
        completeStage()
      }
    })

  }
}
