package ch.descabato.utils

import ch.descabato.core_old.Size
import ch.descabato.utils.Implicits._

import scala.collection.immutable.TreeMap
import scala.ref.SoftReference

object ByteArrayPool extends ByteArrayPool with Utils {
  private val underlying = new RecyclingByteArrayPool()

  override def recycle(array: Array[Byte]): Unit = underlying.recycle(array)

  override def allocate(minimumSize: Int): Array[Byte] = underlying.allocate(minimumSize)

  override def reportRecycling(): Unit = underlying.reportRecycling()
}

trait ByteArrayPool {
  def recycle(content: BytesWrapper): Unit = {
    recycle(content.array)
  }

  def recycle(array: Array[Byte]): Unit

  def allocate(minimumSize: Int): Array[Byte]

  def reportRecycling(): Unit

}

class AllocatingByteArrayPool extends ByteArrayPool with Utils {
  override def recycle(array: Array[Byte]): Unit = {}

  override def allocate(minimumSize: Int): Array[Byte] = {
    Array.ofDim[Byte](minimumSize)
  }

  override def reportRecycling(): Unit = {}
}

class RecyclingByteArrayPool extends ByteArrayPool with Utils {

  private var called: Long = 0
  private var bytes: Long = 0
  private var arrays: TreeMap[Int, Seq[SoftReference[Array[Byte]]]] = TreeMap.empty
  private var rejectedOffers: Long = 0

  def recycle(array: Array[Byte]): Unit = {
    BytesWrapper.synchronized {
      called += 1
      bytes += array.length
      if (array.length > 128 * 1024) {
        if (!arrays.safeContains(array.length)) {
          arrays += array.length -> Seq.empty
        }
        var seq = arrays(array.length)
        seq :+= SoftReference(array)
        arrays += array.length -> seq
      } else {
        rejectedOffers += 1
      }
    }
  }

  private var totalReusedBytes = 0L
  private var totalRequestedBytes = 0L
  private var calledAllocate = 0L

  def allocate(minimumSize: Int): Array[Byte] = {
    BytesWrapper.synchronized {
      calledAllocate += 1
      val it = arrays.iteratorFrom(minimumSize)
      for ((size, seq) <- it) {
        var newSeq = seq
        var found: Option[Array[Byte]] = None
        for (softref <- seq if found.isEmpty) {
          softref match {
            case SoftReference(value) =>
              totalReusedBytes += value.length
              totalRequestedBytes += minimumSize
              logger.info(s"Could reuse byte array of size ${value.length} for requested $minimumSize")
              newSeq = newSeq.tail
              found = Some(value)
            case _ =>
              newSeq = newSeq.tail
          }
          if (newSeq.nonEmpty) {
            arrays += size -> newSeq
          } else {
            arrays -= size
          }
          if (found.isDefined) {
            return found.get
          }
        }
      }
      Array.ofDim(minimumSize)
    }
  }

  def reportRecycling(): Unit = {
    if (called > 0) {
      logger.info(s"Recycle was called $called times and was offered byte arrays with ${Size(bytes)} total (${bytes / called} average)")
      logger.info(s"Allocate was called $calledAllocate times and reused byte arrays with ${Size(totalReusedBytes)} total (${Size(totalReusedBytes - totalRequestedBytes)}) wasted")
      logger.info(s"Rejected $rejectedOffers offers")
    } else {
      logger.info(s"Recycle was never called")
    }
  }

}
