package ch.descabato.core.model

import java.util.Objects
import java.util.concurrent.atomic.AtomicLong

import ch.descabato.core.actors.FilePosition
import ch.descabato.utils.Hash

case class StoredChunk(id: Long, file: String, hash: Hash, startPos: Long, length: Long) {
  def asFilePosition(): FilePosition = {
    FilePosition(startPos, length)
  }

  override def hashCode(): Int = Objects.hashCode(id, file, startPos, length, hash.hashContent())

  override def equals(obj: scala.Any): Boolean = obj match {
    case StoredChunk(o_id, o_file, o_hash, o_startPos, o_length) =>
      o_id == id && o_file == file && o_hash === hash && o_startPos == startPos && o_length == length
    case _ => false
  }

  def ===(other: StoredChunk): Boolean = equals(other)
}


object ChunkIds {
  private val lastId: AtomicLong = new AtomicLong()

  def nextId(): Long = {
    lastId.incrementAndGet()
  }

  def maxId(id: Long): Unit = {
    if (lastId.get() < id) {
      lastId.set(id)
    }
  }

}