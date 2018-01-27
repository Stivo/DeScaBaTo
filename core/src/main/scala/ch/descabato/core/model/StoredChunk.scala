package ch.descabato.core.model

import ch.descabato.core.actors.FilePosition
import ch.descabato.utils.Hash

case class StoredChunk(file: String, hash: Hash, startPos: Long, length: Long) {
  def asFilePosition(): FilePosition = {
    FilePosition(startPos, length)
  }

}
