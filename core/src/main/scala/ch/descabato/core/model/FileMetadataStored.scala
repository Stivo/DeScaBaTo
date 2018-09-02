package ch.descabato.core.model

import java.util

import ch.descabato.core.util.JacksonAnnotations.JsonIgnore
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

case class FileMetadataStored(id: Long, fd: FileDescription,
                              @JsonDeserialize(contentAs = classOf[java.lang.Long]) chunkIds: Array[Long]) extends StoredPartWithPath {
  @JsonIgnore
  def path: String = fd.path

  override def checkIfMatch(other: FileDescription): Boolean = {
    fd.path == other.path && fd.size == other.size && fd.attrs.lastModifiedTime == other.attrs.lastModifiedTime
  }

  override def toString: String = s"$id $fd $chunkIds"

  override def equals(obj: scala.Any) = obj match {
    case FileMetadataStored(o_id, o_fd, o_chunkIds) => o_id == id && fd == o_fd &&
      util.Arrays.equals(o_chunkIds, chunkIds)
    case _ => false
  }
}
