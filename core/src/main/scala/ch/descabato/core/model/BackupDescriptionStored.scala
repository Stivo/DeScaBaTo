package ch.descabato.core.model

import java.util.concurrent.atomic.AtomicLong

import ch.descabato.utils.Hash

import scala.collection.mutable

class BackupDescriptionStored(var fileIds: mutable.Buffer[java.lang.Long] = mutable.Buffer.empty, var dirIds: mutable.Buffer[java.lang.Long] = mutable.Buffer.empty)

case class HashList(id: Long, hash: Hash) extends StoredPart

case class FolderMetadataStored(id: Long, folderDescription: FolderDescription) extends StoredPartWithPath {
  def path = folderDescription.path

  override def checkIfMatch(fd: FileDescription): Boolean = false
}

object BackupIds {
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