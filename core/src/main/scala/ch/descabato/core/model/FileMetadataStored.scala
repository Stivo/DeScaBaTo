package ch.descabato.core.model

import ch.descabato.core_old.FileDescription
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

case class FileMetadataStored(id: Long, fd: FileDescription,
                              @JsonDeserialize(contentAs = classOf[java.lang.Long]) chunkIds: Seq[Long]) extends StoredPartWithPath {
  @JsonIgnore
  def path = fd.path

  override def checkIfMatch(other: FileDescription): Boolean = {
    fd.path == other.path && fd.size == other.size && fd.attrs.lastModifiedTime == other.attrs.lastModifiedTime
  }
}
