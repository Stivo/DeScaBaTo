package ch.descabato.frontend

import org.fusesource.jansi.AnsiConsole
import org.fusesource.jansi.Ansi._
import org.fusesource.jansi.Ansi
import java.util.Date
import scala.collection.mutable.Buffer
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.classic.spi.ILoggingEvent
import org.ocpsoft.prettytime.PrettyTime
import scala.collection.JavaConverters._
import org.ocpsoft.prettytime.format.SimpleTimeFormat
import org.ocpsoft.prettytime.units.JustNow
import ch.descabato.utils.Utils
import javax.swing.SwingUtilities
import ch.descabato.akka.ActorStats

object ProgressReporters {

  var guiEnabled = true

  var gui: Option[ProgressGui] = None

  def openGui() {
    if (!guiEnabled)
      return
    gui.synchronized {
      counters.synchronized {
      SwingUtilities.invokeAndWait(new Runnable() {
        def run() {
          if (gui.isEmpty) {
            gui = Some(CreateProgressGui(ActorStats.tpe.getCorePoolSize))
            counters.filterNot(_._1.contains("iles found")).values.foreach{gui.get.add}
          }
        }
      })
      }
    }
  }

  val reporter = ConsoleManager

  def addCounter(c: Counter) {
    counters.synchronized {
    if (counters.get(c.name).isDefined)
      return
    counters += c.name -> c
    for (g <- gui)
      g.add(c)
    }
  }

  def getCounter(name: String) = counters(name)

  private var counters = Map[String, Counter]()

  def updateWithCounters(counters: Seq[Counter]) {
    counters.foreach{ addCounter }
    reporter.ephemeralMessage {
      counters.map(_.nameAndValue).mkString(" ")
    }
  }

  def newPrettyTime() = {
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

}

trait ProgressReporting {

  def ephemeralMessage(message: => String)

}

trait MaxValueCounter extends Counter {
  var maxValue = 0L

  override def formatted = s"$current/$maxValue"

  def percent = if (maxValue == 0) 0 else (100 * current / maxValue).toInt
}

trait UpdatingCounter extends Counter {
  def update()
  abstract override def allowed() = {
    val out = super.allowed()
    if (out)
      update
    out
  }
}

trait Counter {
  private var nextTime = 0L
  var lastCurrent = -1L
  def allowed() = {
    if (System.currentTimeMillis() > nextTime) {
      nextTime = System.currentTimeMillis() + 5
      lastCurrent = current
      true
    } else {
      false
    }
  }

  def name: String
  var current = 0L

  def +=(l: Long) {
    this.synchronized {
      current += l
    }
  }

  def formatted = s"$current"

  def nameAndValue = s"$name $formatted"
}

class StandardCounter(val name: String) extends Counter

trait ETACounter extends MaxValueCounter with Utils {
  def window = 60
  case class Snapshot(time: Long, l: Double)
  val p = ProgressReporters.newPrettyTime()
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
    readableFileSize((1000 * rate).toLong) + "/s, ETF: " +
      p.format(new Date(System.currentTimeMillis() + ms.toLong))
  }

  override def formatted = super.formatted + " " + calcEta
}

class StandardMaxValueCounter(val name: String, maxValueIn: Long) extends MaxValueCounter {
  maxValue = maxValueIn
}

object AnsiUtil {
  var ansiDisabled = false
  lazy val initAnsi = if (!ansiDisabled && CLI.runsInJar && System.getProperty("user.name").toLowerCase() != "travis") {
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

  def ephemeralMessage(message: => String) {
    val timeNow = System.currentTimeMillis()
    if (timeNow > lastOne) {
      if (AnsiUtil.deleteLinesEnabled) {
        ConsoleManager.appender.writeDeleteLine(message)
        lastOne = timeNow + 20
      } else {
        ConsoleManager.appender.writeDeleteLine(message + "\n", false)
        lastOne = timeNow + 5000
      }
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

  def writeDeleteLine(message: String, delete: Boolean = true) {
    lock.synchronized {
      if (!delete) {
        canDeleteLast = false
      }
      canDeleteLast match {
        case true => sendAnsiLine(message)
        case false => writeSafe(message)
      }
      canDeleteLast = true
    }
  }

  private def eraseLast = ansi().cursorLeft(100).eraseLine(Erase.FORWARD)

  private def sendAnsiLine(message: String) {
    writeSafe(eraseLast.a(message).toString)
  }

}
