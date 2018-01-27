package ch.descabato.core.model

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

class BackupDescription(var fileIds: mutable.Buffer[Long] = mutable.Buffer.empty, var dirIds: mutable.Buffer[Long] = mutable.Buffer.empty)

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