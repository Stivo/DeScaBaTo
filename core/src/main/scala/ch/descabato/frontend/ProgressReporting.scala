package ch.descabato.frontend

import ch.descabato.core.ActorStats
import ch.descabato.core.model.Size
import ch.descabato.remote.RemoteOptions
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Utils
import org.ocpsoft.prettytime.PrettyTime
import org.ocpsoft.prettytime.format.SimpleTimeFormat
import org.ocpsoft.prettytime.units.JustNow

import java.util.Date
import java.util.concurrent.atomic.AtomicLong
import javax.swing.SwingUtilities
import scala.jdk.CollectionConverters._

object ProgressReporters {

  var guiEnabled = true

  var gui: Option[ProgressGui] = None

  def openGui(nameOfOperation: String, sliderDisabled: Boolean = false, remoteOptions: RemoteOptions = null): Unit = {
    if (!guiEnabled) {
      return
    }
    gui.synchronized {
      counters.synchronized {
        SwingUtilities.invokeAndWait(() => {
          if (gui.isEmpty) {
            gui = Some(CreateProgressGui(ActorStats.tpe.getCorePoolSize, nameOfOperation, sliderDisabled, remoteOptions))
            counters.filterNot(_._1.contains("iles found")).values.foreach {
              gui.get.add
            }
          }
        })
      }
    }
  }

  def addCounter(newCounters: Counter*) {
    counters.synchronized {
      for (c <- newCounters if !(counters safeContains c.name)) {
        counters += c.name -> c
        for (g <- gui)
          g.add(c)
      }
    }
  }

  def getCounter(name: String) = counters(name)

  private var counters = Map[String, Counter]()

  var activeCounters: Seq[Counter] = List()

  def newPrettyTime(): PrettyTime = {
    val out = new PrettyTime()
    val justNow = out.getUnits.asScala.find {
      case u: JustNow => true
      case _ => false
    }.get
    out.removeUnit(justNow)
    out.getUnits.asScala.map(x => (x, out.getFormat(x))).foreach {
      case (_, x: SimpleTimeFormat) => x.setFutureSuffix("")
    }
    out
  }

  def consoleUpdate(names: Boolean = false): String = {
    activeCounters.map {
      case x: ETACounter => s"""${if (names) x.name + ": " else ""}${x.formattedWithEta}"""
      case x => s"""${if (names) x.name + ": " else ""}${x.formatted}"""
    }.mkString(" --- ")
  }

}

trait MaxValueCounter extends Counter {
  private var _maxValue = new AtomicLong()

  def maxValue: Long = _maxValue.get()

  def maxValue_=(newValue: Long): Unit = _maxValue.set(newValue)

  def maxValue_+=(newValue: Long): Unit = _maxValue.addAndGet(newValue)

  override def formatted = s"$current / $maxValue (${percent}%)"

  def percent: Int = if (maxValue == 0) 0 else (100 * current / maxValue).toInt
}

trait Counter {
  def name: String

  private val _current = new AtomicLong()

  def current: Long = _current.get

  def current_=(newValue: Long): Unit = _current.set(newValue)

  def +=(l: Long): Unit = {
    _current.addAndGet(l)
  }

  def update(): Unit = {}

  def formatted: String = {
    update()
    s"$current"
  }

  def nameAndValue = s"$name $formatted"
}

class StandardCounter(val name: String) extends Counter

class StandardByteCounter(val name: String) extends Counter {
  override def formatted: String = Size(current).toString
}

trait ETACounter extends MaxValueCounter with Utils {
  def window = 60

  case class Snapshot(time: Long, l: Double)

  val p: PrettyTime = ProgressReporters.newPrettyTime()
  var snapshots: List[Snapshot] = List[Snapshot]()
  var newSnapshotAt = 0L

  def resetSnapshots(): Unit = {
    snapshots = List.empty
  }

  override def +=(l: Long): Unit = {
    super.+=(l)
    val now = System.currentTimeMillis()
    if (newSnapshotAt < now) {
      snapshots :+= Snapshot(now, current.toDouble)
      val cutoff = now - window * 1000
      snapshots = snapshots.dropWhile(_.time < cutoff)
      newSnapshotAt = now + 100
    }
  }

  def calcEta: String = {
    if (snapshots.lengthCompare(2) < 0) {
      return ""
    }
    val last = snapshots.last
    val first = snapshots.head
    val rate = (last.l - first.l) / (last.time - first.time)
    val remaining: Double = maxValue - last.l
    val ms = remaining / rate
    readableFileSize((1000 * rate).toLong) + "/s, ETF: " +
      p.format(new Date(System.currentTimeMillis() + ms.toLong))
  }

  def formattedWithEta: String = formatted + " " + calcEta
}

class SizeStandardCounter(val name: String) extends MaxValueCounter with ETACounter {
  override def formatted: String = {
    s"${Utils.readableFileSize(current)} / ${Utils.readableFileSize(maxValue)}"
  }
}

class FileCounter extends SizeStandardCounter("filename") {
  var fileName: String = "filename"

  override def formatted = s"$fileName ${super[SizeStandardCounter].formatted} $calcEta"
}

class StandardMaxValueCounter(val name: String, maxValueIn: Long) extends MaxValueCounter {
  maxValue = maxValueIn
}
