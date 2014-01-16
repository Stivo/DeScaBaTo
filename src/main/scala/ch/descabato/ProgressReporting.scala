package ch.descabato

import org.fusesource.jansi.AnsiConsole
import org.fusesource.jansi.Ansi._
import org.fusesource.jansi.Ansi
import java.util.Date
import scala.collection.mutable.Buffer
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.classic.spi.ILoggingEvent
import org.ocpsoft.prettytime.PrettyTime
import org.ocpsoft.prettytime.Duration
import org.ocpsoft.prettytime.impl.DurationImpl
import org.ocpsoft.prettytime.TimeUnit
import org.ocpsoft.prettytime.units.Millisecond
import scala.collection.JavaConverters._
import org.ocpsoft.prettytime.format.SimpleTimeFormat

object ProgressReporters {

  val reporter = ConsoleManager

  def addCounter(c: Counter) {
    counters += c.name -> c
  }

  def getCounter(name: String) = counters(name)

  private var counters = Map[String, Counter]()

  def updateWithCounters(counters: Seq[Counter]) {
    val string = counters.map(_.update).mkString(" ")
    reporter.ephemeralMessage(string)
  }

}

trait ProgressReporting {

  def ephemeralMessage(message: String)

}

trait Counter {
  def name: String
  var current = 0L
  var maxValue = 0L

  def format = s"$current/$maxValue"

  def percent = (100 * current / maxValue).toInt

  def update = s"$name: $format"

  def +=(l: Long) { current += l }
}

trait ETACounter extends Counter with Utils {
  def window = 60
  case class Snapshot(time: Long, l: Double)
  val p = new PrettyTime();
  val tu = p.getUnits().asScala.foreach {
    p.getFormat(_) match {
      case x: SimpleTimeFormat => x.setFutureSuffix ("")
      case _ =>
    }
  }
  var snapshots = List[Snapshot]()
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
    if (snapshots.size < 2) {
      return ""
    }
    val last = snapshots.last
    val first = snapshots.head
    val rate = (last.l - first.l) / (last.time - first.time)
    val remaining = (maxValue - last.l).toDouble
    val ms = remaining / rate
    readableFileSize((1000*rate).toLong)+"/s, ETA: "+p.format(new Date(System.currentTimeMillis()+ms.toLong))
  }

  override def update = super.update + " " + calcEta
}

class StandardCounter(val name: String, maxValueIn: Long) extends Counter {
  maxValue = maxValueIn
}

object ProgressReporter extends ProgressReporting {
  def ephemeralMessage(message: String) {}
  def setTotalPercent(x: Int) {}
}

object AnsiUtil {
  lazy val initAnsi = if (CLI.runsInJar) {
    AnsiConsole.systemInstall();
  } else {
    deleteLinesEnabled = false
  }

  var deleteLinesEnabled = true

  def mark(s: String, pattern: String, color: AnsiColor = red) = {
    var stringParts = Buffer[String]()
    var args = Buffer[Any]()
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

  def disableAnsi() = Ansi.setEnabled(false)
  def testSetup() {
    disableAnsi
    new ConsoleAppenderWithDeleteSupport()
  }

  import Color._

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
        wasAnsi = true;
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

object ConsoleManager extends ProgressReporting {
  var appender: ConsoleAppenderWithDeleteSupport = null

  var lastOne = 0L

  def ephemeralMessage(message: String) {
    if (!AnsiUtil.deleteLinesEnabled)
      return
    val timeNow = new Date().getTime()
    if (timeNow > lastOne) {
      ConsoleManager.appender.writeDeleteLine(message)
      lastOne = timeNow + 10
    }
  }

}

class ConsoleAppenderWithDeleteSupport extends ConsoleAppender[ILoggingEvent] {
  AnsiUtil.initAnsi
  ConsoleManager.appender = this

  @volatile var canDeleteLast = false

  override def append(t: ILoggingEvent) {
    lock.synchronized {
      if (canDeleteLast) {
        writeSafe(eraseLast.toString())
      }
      super.append(t)
      canDeleteLast = false
    }
  }

  private def writeSafe(x: String) = lock.synchronized {
    super.getOutputStream().write(x.getBytes())
    getOutputStream().flush()
  }

  def writeDeleteLine(message: String) {
    if (!Ansi.isEnabled()) {
      return
    }
    canDeleteLast match {
      case true => sendAnsiLine(message)
      case false => writeSafe(message)
    }
    canDeleteLast = true
  }

  def eraseLast = ansi().cursorLeft(100).eraseLine(Erase.FORWARD)

  def sendAnsiLine(message: String) {
    writeSafe(eraseLast.a(message).toString)
  }

}
