package ch.descabato.core.model

import ch.descabato.core_old.FileDescription
import ch.descabato.utils.Hash

case class FileMetadataStored(id: Long, fd: FileDescription, blocks: Hash) extends StoredPartWithPath {
  def path = fd.path

  override def checkIfMatch(other: FileDescription): Boolean = {
    fd.path == other.path && fd.size == other.size && fd.attrs.get("lastModifiedTime") == other.attrs.get("lastModifiedTime")
  }
}
