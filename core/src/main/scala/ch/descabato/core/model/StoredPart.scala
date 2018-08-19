package ch.descabato.core.model

trait StoredPart {

  def id: Long

}

trait StoredPartWithPath extends StoredPart {
  def checkIfMatch(fd: FileDescription): Boolean

  def path: String

}
