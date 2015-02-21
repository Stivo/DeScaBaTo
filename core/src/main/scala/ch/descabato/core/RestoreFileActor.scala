package ch.descabato.core

import java.io.{File, FileOutputStream, OutputStream}

import akka.actor.{PoisonPill, TypedActor}
import akka.io.Tcp.Write
import ch.descabato.akka.AkkaUniversePart
import ch.descabato.utils.{CompressedStream, Utils}

import scala.collection.{SortedMap, mutable}
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._

trait AkkaRestoreFileHandler extends RestoreFileHandler {
  def setup(fd: FileDescription, dest: File, ownRef: AkkaRestoreFileHandler)
  def blockDecompressed(block: Block)
}
class RestoreFileActor extends AkkaRestoreFileHandler with Utils with AkkaUniversePart {
  val maxPending: Int = 100
  val p = Promise[Boolean]
  lazy val digest = config.getMessageDigest

  var writeFileHandler: WriteFileHandler = null
  var ownRef: AkkaRestoreFileHandler = null
  var fd: FileDescription = null

  var unwrittenBlocks = SortedMap.empty[Int, Block]
  var pendingBlocks = mutable.Set.empty[Int]
  var outputStream: OutputStream = null;
  var nextBlockToRequest: Int = 0
  var blockToBeWritten = 0
  var hashList: Array[Array[Byte]] = null
  var numberOfBlocks = 0

  def setup(fd: FileDescription, dest: File, ownRef: AkkaRestoreFileHandler): Unit = {
    this.fd = fd
    this.ownRef = ownRef
    writeFileHandler = uni.actorOf[WriteFileHandler, WriteActor]("write "+dest)
    writeFileHandler.setFile(dest)
  }

  override def restore(): Future[Boolean] = {
    try {
      startRestore()
      p.future
    }
    catch {
      case e =>
        Future.failed(e)
    }
  }

  def fillUpQueue() {
    while (unwrittenBlocks.size + pendingBlocks.size < maxPending && nextBlockToRequest < numberOfBlocks) {
      val id = new BlockId(fd, nextBlockToRequest)
      pendingBlocks += nextBlockToRequest
      universe.blockHandler().readBlockAsync(hashList(nextBlockToRequest)).map({ bytes =>
        universe.scheduleTask { () =>
          val decomp = CompressedStream.decompressToBytes(bytes)
          ownRef.blockDecompressed(new Block(id, decomp))
        }
      })
      nextBlockToRequest += 1
    }
  }

  def startRestore() = {
    if (fd.size > config.blockSize.bytes) {
      hashList = universe.hashListHandler().getHashlist(fd.hash, fd.size).toArray
    } else {
      hashList = Array.apply(fd.hash)
    }
    numberOfBlocks = hashList.length

    fillUpQueue()
  }

  override def blockDecompressed(block: Block): Unit = {
    // update pending blocks and unwritten blocks
    pendingBlocks -= block.id.part
    unwrittenBlocks += (block.id.part -> block)

    // if block can be written, write it
    while (!unwrittenBlocks.isEmpty && unwrittenBlocks.keySet.min == blockToBeWritten) {
      val array = unwrittenBlocks.head._2
      unwrittenBlocks = unwrittenBlocks.tail
      blockToBeWritten += 1
      writeFileHandler.write(array.content)
    }
    // if pendingblocks is smaller than maxPending and still enough to go: enqueue another one
    fillUpQueue()
    if (blockToBeWritten == numberOfBlocks) {
      // if finished:
      //    fulfill the promise
      val result = Await.result(writeFileHandler.finish(), 5.minutes)
      TypedActor.get(uni.system).getActorRefFor(writeFileHandler) ! PoisonPill
//      if (!util.Arrays.equals(fd.hash, digest.digest())) {
//        l.warn("Could not reconstruct file " + destination.getAbsolutePath)
//        p.success(false)
//      } else {
        p.success(result)
//      }
      TypedActor.get(uni.system).getActorRefFor(ownRef) ! PoisonPill
    }
  }

}

trait WriteFileHandler {
  def setFile(file: File)
  def write(bytes: Array[Byte])
  def finish(): Future[Boolean]
}

class WriteActor extends WriteFileHandler with AkkaUniversePart {
  var fos: FileOutputStream = null
  override def setFile(destination: File): Unit = {
    destination.getParentFile.mkdirs()
    fos = new FileOutputStream(destination)
  }

  override def finish(): Future[Boolean] = {
    try {
      fos.close()
      Future.successful(true)
    } catch {
      case e => Future.failed(e)
    }
  }

  override def write(bytes: Array[Byte]): Unit = {
    fos.write(bytes)
  }
}