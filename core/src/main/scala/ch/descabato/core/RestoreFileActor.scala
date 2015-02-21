package ch.descabato.core

import java.io.{File, FileOutputStream, OutputStream}

import akka.actor.{PoisonPill, TypedActor}
import akka.io.Tcp.Write
import ch.descabato.akka.{AkkaHashActor, AkkaUniversePart}
import ch.descabato.utils.{BytesWrapper, Hash, CompressedStream, Utils}

import scala.collection.{SortedMap, mutable}
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import ch.descabato.utils.Implicits._

trait AkkaRestoreFileHandler extends RestoreFileHandler {
  def setup(fd: FileDescription, dest: File, ownRef: AkkaRestoreFileHandler)
  def blockDecompressed(block: Block)
}

class RestoreFileActor extends AkkaRestoreFileHandler with Utils with AkkaUniversePart {
  val maxPending: Int = 100
  val p = Promise[Boolean]
  lazy val digest = config.createMessageDigest

  // initialized in setup
  var hashHandler: HashHandler = null
  var writeFileHandler: WriteFileHandler = null
  var ownRef: AkkaRestoreFileHandler = null
  var fd: FileDescription = null
  var destination: File = null

  // initialized in startRestore
  var hashList: Array[Hash] = null
  var outputStream: OutputStream = null
  
  var unwrittenBlocks = SortedMap.empty[Int, Block]
  var pendingBlocks = mutable.Set.empty[Int]
  var nextBlockToRequest: Int = 0
  var blockToBeWritten = 0
  var numberOfBlocks = 0

  def setup(fd: FileDescription, dest: File, ownRef: AkkaRestoreFileHandler): Unit = {
    this.fd = fd
    this.ownRef = ownRef
    writeFileHandler = uni.actorOf[WriteFileHandler, WriteActor]("write "+dest)
    writeFileHandler.setFile(dest)
    hashHandler = uni.actorOf[HashHandler, AkkaHashActor]("hash "+dest)
    destination = dest
  }

  override def restore(): Future[Boolean] = {
    try {
      startRestore()
      p.future
    }
    catch {
      case e : Exception =>
        Future.failed(e)
    }
  }

  def fillQueue() {
    while (unwrittenBlocks.size + pendingBlocks.size < maxPending && nextBlockToRequest < numberOfBlocks) {
      val id = new BlockId(fd, nextBlockToRequest)
      pendingBlocks += nextBlockToRequest
      val blockHash = hashList(nextBlockToRequest)
      universe.blockHandler().readBlockAsync(blockHash).map({ bytes =>
        universe.scheduleTask { () =>
          val decomp = CompressedStream.decompressToBytes(bytes)
          val hash = universe.config().createMessageDigest().finish(decomp)
          if (!(hash safeEquals blockHash)) {
            l.warn("Could not reconstruct block "+id+" of file "+destination+" correctly, hash was incorrect")
          }
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

    fillQueue()
  }

  override def blockDecompressed(block: Block): Unit = {
    // update pending blocks and unwritten blocks
    pendingBlocks -= block.id.part
    unwrittenBlocks += (block.id.part -> block)

    writeReadyBlocks()
    fillQueue()

    if (blockToBeWritten == numberOfBlocks) {
      finish()
    }
  }

  def writeReadyBlocks() {
    // if block can be written, write it
    while (unwrittenBlocks.nonEmpty && unwrittenBlocks.keySet.min == blockToBeWritten) {
      val array = unwrittenBlocks.head._2
      unwrittenBlocks = unwrittenBlocks.tail
      blockToBeWritten += 1
      writeFileHandler.write(array.content)
      add1
      hashHandler.hash(array.content)
    }
  }

  def finish(): Unit = {
    // if finished: wait for write actor and kill it, wait for hash result, fulfill promise
    val result = Await.result(writeFileHandler.finish(), 5.minutes)
    TypedActor.get(uni.system).getActorRefFor(writeFileHandler) ! PoisonPill
    val hashPromise = Promise[Hash]
    hashHandler.finish{ hash =>
      hashPromise.success(hash)
    }
    val computedHash = Await.result(hashPromise.future, 5.minutes)
    TypedActor.get(uni.system).getActorRefFor(hashHandler) ! PoisonPill
    //    fulfill the promise
    if (! (fd.hash safeEquals computedHash)) {
      l.warn("Could not reconstruct file exactly " + destination.getAbsolutePath)
      p.success(false)
    } else {
      p.success(result)
    }
    TypedActor.get(uni.system).getActorRefFor(ownRef) ! PoisonPill

  }
}

trait WriteFileHandler {
  def setFile(file: File)
  def write(bytes: BytesWrapper)
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
      case e : Exception => Future.failed(e)
    }
  }

  override def write(bytes: BytesWrapper): Unit = {
    fos.write(bytes)
  }
}