package ch.descabato.frontend

import java.util.concurrent.atomic.AtomicLong
import java.util.{Date, TimerTask}
import javax.swing.SwingUtilities

import ch.descabato.core.ActorStats
import ch.descabato.core_old.Size
import ch.descabato.remote.RemoteOptions
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Utils
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.ConsoleAppender
import org.fusesource.jansi.Ansi._
import org.fusesource.jansi.{Ansi, AnsiConsole}
import org.ocpsoft.prettytime.PrettyTime
import org.ocpsoft.prettytime.format.SimpleTimeFormat
import org.ocpsoft.prettytime.units.JustNow

import scala.collection.JavaConverters._
import scala.collection.mutable

object ProgressReporters {

  var guiEnabled = true

  var gui: Option[ProgressGui] = None

  def openGui(nameOfOperation: String, sliderDisabled: Boolean = false, remoteOptions: RemoteOptions = null) {
    if (!guiEnabled)
      return
    gui.synchronized {
      counters.synchronized {
        SwingUtilities.invokeAndWait(new Runnable() {
          def run() {
            if (gui.isEmpty) {
              gui = Some(CreateProgressGui(ActorStats.tpe.getCorePoolSize, nameOfOperation, sliderDisabled, remoteOptions))
              counters.filterNot(_._1.contains("iles found")).values.foreach {
                gui.get.add
              }
            }
          }
        })
      }
    }
  }

  val reporter: ConsoleManager.type = ConsoleManager

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

trait ProgressReporting {

  def ephemeralMessage(message: => String)

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

  private var _current = new AtomicLong()

  def current: Long = _current.get

  def current_=(newValue: Long): Unit = _current.set(newValue)

  def +=(l: Long): Unit = {
    _current.addAndGet(l)
  }

  def update() {}

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

  override def +=(l: Long) {
    super.+=(l)
    val now = System.currentTimeMillis()
    if (newSnapshotAt < now) {
      snapshots :+= Snapshot(now, current)
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

object AnsiUtil {
  var ansiDisabled = false
  lazy val initAnsi: Unit = if (!ansiDisabled && CLI.runsInJar && System.getProperty("user.name").toLowerCase() != "travis") {
    AnsiConsole.systemInstall()
  } else {
    deleteLinesEnabled = false
  }

  var deleteLinesEnabled = true

  def mark(s: String, pattern: String, color: AnsiColor = red): String = {
    var stringParts = mutable.Buffer[String]()
    var args = mutable.Buffer[Any]()
    var todo = s
    while (todo.contains(pattern)) {
      val start = todo.indexOf(pattern)
      stringParts += todo.take(start)
      args += color
      todo = todo.drop(start)
      stringParts += todo.take(pattern.length())
      args += reset
      todo = todo.drop(pattern.length())
    }
    stringParts += todo
    new AnsiHelper(new StringContext(stringParts: _*)).a(args: _*)
  }

  def disableAnsi(): Unit = Ansi.setEnabled(false)

  def testSetup() {
    disableAnsi
    new ConsoleAppenderWithDeleteSupport()
  }

  import org.fusesource.jansi.Ansi.Color._

  trait AnsiCharacter

  case object Up extends AnsiCharacter

  class AnsiColor(val c: Color) extends AnsiCharacter

  case object green extends AnsiColor(GREEN)

  case object white extends AnsiColor(WHITE)

  case object yellow extends AnsiColor(YELLOW)

  case object blue extends AnsiColor(BLUE)

  case object magenta extends AnsiColor(MAGENTA)

  case object cyan extends AnsiColor(CYAN)

  case object black extends AnsiColor(BLACK)

  case object red extends AnsiColor(RED)

  case object default extends AnsiColor(DEFAULT)

  case object reset extends AnsiCharacter

  implicit class AnsiHelper(val sc: StringContext) extends AnyVal {

    def a(args: Any*): String = {
      val s = sc.parts.iterator
      val first = s.next
      var out = ansi().a(first)
      val as = args.iterator
      var lastHadSpace = first.endsWith(" ")
      var wasAnsi = false
      while (s.hasNext) {
        val arg = as.next
        wasAnsi = true
        arg match {
          case c: AnsiColor =>
            out = out.fg(c.c)
          case Up =>
            out = out.cursorUp(1).cursorLeft(1000)
          case `reset` =>
            out = out.reset()
          case x =>
            wasAnsi = false
            out = out.a(x)
        }
        var next = s.next
        if (lastHadSpace && wasAnsi && next.startsWith(" "))
          next = next.drop(1)
        out = out.a(next)
        lastHadSpace = next.endsWith(" ")
      }
      out = out.fg(default.c) //.restorCursorPosition()
      //out = out.cursorRight(100)
      out.toString
    }
  }

}

object ConsoleManager extends ProgressReporting with Utils {
  var appender: ConsoleAppenderWithDeleteSupport = _

  def ephemeralMessage(message: => String) {
    appender.writeDeleteLine(message)
  }

  lazy val timer = new java.util.Timer()

  class RepeatingWithTimeDelayTask(function: => Unit, period: Int) extends TimerTask {
    def run() {
      function
      timer.schedule(new RepeatingWithTimeDelayTask(function, period), period)
    }
  }

  def startConsoleUpdater() {
    if (AnsiUtil.deleteLinesEnabled) {
      timer.schedule(new RepeatingWithTimeDelayTask({
        val status = updateConsoleStatus()
        if (status != "")
          ephemeralMessage(status)
      }, 100), 1000)
    }
    timer.schedule(new RepeatingWithTimeDelayTask({
      val status = updateConsoleStatus(names = true)
      if (status != "")
        l.info(status)
    }, 10000), 1000)
  }

  def updateConsoleStatus(names: Boolean = false): String = {
    ProgressReporters.consoleUpdate(names)
  }

}

class ConsoleAppenderWithDeleteSupport extends ConsoleAppender[ILoggingEvent] {
  AnsiUtil.initAnsi
  ConsoleManager.appender = this

  var canDeleteLast = false

  override def append(t: ILoggingEvent) {
    lock.synchronized {
      if (canDeleteLast) {
        writeSafe(eraseLast.toString())
      }
      super.append(t)
      canDeleteLast = false
    }
  }

  private def writeSafe(x: String): Unit = lock.synchronized {
    super.getOutputStream().write(x.getBytes())
    getOutputStream().flush()
  }

  def writeDeleteLine(message: String, delete: Boolean = true) {
    lock.synchronized {
      if (!delete) {
        canDeleteLast = false
      }
      if (canDeleteLast && delete) {
        sendAnsiLine(message)
      } else {
        writeSafe(message)
      }
      canDeleteLast = true
    }
  }

  private def eraseLast = ansi().cursorLeft(100).eraseLine(Erase.FORWARD)

  private def sendAnsiLine(message: String) {
    writeSafe(eraseLast.a(message).toString)
  }

}
