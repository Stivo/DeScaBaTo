package ch.descabato.core.model

import ch.descabato.core_old.FileDescription
import ch.descabato.utils.Hash
import com.fasterxml.jackson.annotation.JsonIgnore

case class FileMetadata(id: Long, fd: FileDescription, hashListId: Hash) {
  @JsonIgnore
  var blocks: Hash = _
}
