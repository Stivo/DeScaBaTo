package ch.descabato.core.model

import ch.descabato.core_old.FileDescription
import ch.descabato.utils.Hash
import com.fasterxml.jackson.annotation.JsonIgnore

case class FileMetadataStored(id: Long, fd: FileDescription, hashListId: Long) extends StoredPartWithPath {
  @JsonIgnore
  def path = fd.path

  @JsonIgnore
  var blocks: Hash = _

  override def checkIfMatch(other: FileDescription): Boolean = {
    fd.path == other.path && fd.size == other.size && fd.attrs.get("lastModifiedTime") == other.attrs.get("lastModifiedTime")
  }
}
