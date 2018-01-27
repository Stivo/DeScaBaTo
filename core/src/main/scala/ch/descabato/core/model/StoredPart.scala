package ch.descabato.core.model

import ch.descabato.core_old.FileDescription

trait StoredPart {

  def id: Long

}

trait StoredPartWithPath extends StoredPart {
  def checkIfMatch(fd: FileDescription): Boolean

  def path: String

}
