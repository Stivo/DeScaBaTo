package ch.descabato.core.model

import java.util.concurrent.atomic.AtomicLong

import ch.descabato.core.actors.FilePosition
import ch.descabato.utils.Hash

case class StoredChunk(id: Long, file: String, hash: Hash, startPos: Long, length: Long) {
  def asFilePosition(): FilePosition = {
    FilePosition(startPos, length)
  }

}


object ChunkIds {
  private var lastId: AtomicLong = new AtomicLong()

  def nextId(): Long = {
    lastId.incrementAndGet()
  }

  def maxId(id: Long): Unit = {
    if (lastId.get() < id) {
      lastId.set(id)
    }
  }

}