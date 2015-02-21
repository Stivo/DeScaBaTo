package ch.descabato.utils

import java.io.OutputStream

import ch.descabato.ByteArrayOutputStream

import scala.collection.mutable
import scala.ref.WeakReference

object ObjectPools extends Utils {

  var foundExactCounter = 0
  var foundExactRequests = 0
  var foundMinimumCounter = 0
  var foundMinimumRequests = 0

  def printStatistics(size: Int) {
//    l.info("Found minimum "+foundMinimumCounter+" / "+foundMinimumRequests)
//    l.info("Found exact "+foundExactCounter+" / "+foundExactRequests)
//    l.info("Current Weak References "+size)
  }

  class ByteArrayObjectPool {
    type T = Array[Byte]
    val local = new ThreadLocal[mutable.Buffer[WeakReference[T]]]
    def getStack() = {
      val out = local.get()
      if (out == null) {
        val n = mutable.Buffer[WeakReference[T]]()
        local.set(n)
        n
      } else {
        out
      }
    }
    final def getMinimum(arg: Int): T = {
      def isValidForArg(x: T) = x.length >= arg
      if (arg < 1024) {
        return makeNew(arg)
      }
      val found = get(isValidForArg)
//      l.info(s"Found byte array with minimum $arg ${found.isDefined}")
      if (found.isDefined) {
        foundMinimumCounter += 1
      }
      foundMinimumRequests += 1
      if (foundMinimumRequests % 1000 == 0) {
        printStatistics(getStack.size)
      }
      found getOrElse makeNew(arg)
    }

    final def getExactly(arg: Int): T = {
      def isValidForArg(x: T) = x.length == arg
      if (arg < 1024) {
        return makeNew(arg)
      }
      val found = get(isValidForArg)
//      l.info(s"Found byte array with exactly $arg ${found.isDefined}")
      if (found.isDefined) {
        foundExactCounter += 1
      }
      foundExactRequests += 1
      found getOrElse makeNew(arg)
    }

    private def get(f: T => Boolean): Option[T] = {
      val stack = getStack()
      var strongRef: Option[T] = None
      while (stack.nonEmpty) {
        val toRemove = mutable.Buffer[WeakReference[T]]()
        val result = stack.find {
          case wr@WeakReference(x) if f(x) =>
            // before x is garbage collected, save it to a strong reference
            strongRef = Some(x)
            toRemove += wr
            true
          case WeakReference(x) => false // continue searching
          case wr => // This is an empty reference now, we might as well delete it
            toRemove += wr; false
        }
        stack --= toRemove
        return strongRef
      }
      None
    }


    final def recycle(t: T) {
//      val stack = getStack
//      stack += (WeakReference(t))
    }
    protected def makeNew(arg: Int): T = {
      Array.ofDim[Byte](arg)
    }
  }

  val byteArrayPool = new ByteArrayObjectPool

}

object Streams extends Utils {

  class BlockOutputStream(val blockSize: Int, func: (Array[Byte] => _)) extends OutputStream {

    var out = new ByteArrayOutputStream(blockSize)

    var funcWasCalledOnce = false

    def write(b: Int) {
      out.write(b)
      handleEnd()
    }

    def handleEnd() {
      if (cur == blockSize) {
        funcWasCalledOnce = true
        func(out.toByteArray())
        out.reset()
      }
    }

    def cur = out.size()

    override def write(buf: Array[Byte], start: Int, len: Int) {
      var (lenC, startC) = (len, start)
      while (lenC > 0) {
        val now = Math.min(blockSize - cur, lenC)
        out.write(buf, startC, now)
        lenC -= now
        startC += now
        handleEnd()
      }
    }

    override def close() {
      if (out.size() > 0 || !funcWasCalledOnce)
        func(out.toByteArray())
      super.close()
      out.recycle()
      // out was returned to the pool, so remove instance pointer
      out = null
    }

  }

}
