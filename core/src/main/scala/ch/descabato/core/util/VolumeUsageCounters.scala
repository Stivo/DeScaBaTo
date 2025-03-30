package ch.descabato.core.util

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.protobuf.keys.ValueLogIndex

import scala.collection.immutable.SortedMap

case class VolumeUsageReport(volume: String, length: Long, unusedBytes: Long) {
  def percentUnused: Double = unusedBytes * 100.0 / length
}

class VolumeUsageCounters(config: BackupFolderConfiguration) {

  private var counters: SortedMap[String, VolumeUsageCounter] = SortedMap.empty

  new FileManager(config).volume.getFiles().foreach { file =>
    counters += file.getName -> new VolumeUsageCounter(file.getName, file.length())
  }

  def addChunk(c: ValueLogIndex): Unit = {
    val volumeName = c.filename.split("[/\\\\]").last
    counters(volumeName).addChunk(c)
  }

  def reportAllVolumes(): Seq[VolumeUsageReport] = {
    counters.values.map { x =>
      VolumeUsageReport(x.filename, x.length, x.reportUnusedBytes())
    }.toSeq
  }
}

class VolumeUsageCounter(val filename: String, val length: Long) {

  private var usageTrackingMap: SortedMap[Long, UsageToken] = SortedMap.empty

  usageTrackingMap += 0L -> Unused
  usageTrackingMap += length -> End


  def addChunk(c: ValueLogIndex): Unit = {
    if (c.filename.split("[/\\\\]").last != filename) {
      throw new IllegalArgumentException(s"Wrong file ${c.filename}, this tracker is for $filename")
    }

    val start = c.from
    val end = c.from + c.lengthCompressed
    usageTrackingMap += start -> Used
    val newNode = usageTrackingMap.get(end) match {
      case None =>
        Unused
      case Some(x@(End | Used)) =>
        x
      case Some(Unused) =>
        throw new IllegalArgumentException(s"Already an unused token at $start")
    }
    usageTrackingMap += end -> newNode
    //println(usageTrackingMap)
  }

  def reportUnusedBytes(): Long = {
    var unusedBytes = 0L
    for ((k1: Long, v1) :: (k2: Long, v2) :: _ <- usageTrackingMap.toSeq.sliding(2)) {
      //      println(s"$k1 $v1 -> $k2 $v2")
      v1 match {
        case Unused =>
          unusedBytes += k2 - k1
        case _ =>
      }
    }
    if (unusedBytes < 100) {
      0
    } else {
      unusedBytes
    }
  }

  def reportAll(): Unit = {
    usageTrackingMap.foreach(println)
  }

}

sealed trait UsageToken

case object Unused extends UsageToken

case object End extends UsageToken

case object Used extends UsageToken