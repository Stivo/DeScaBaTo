package backup

import ch.qos.logback.core.AppenderBase
import ch.qos.logback.classic.spi.ILoggingEvent
import scala.collection.mutable.Buffer
import org.fusesource.jansi.AnsiConsole
import org.fusesource.jansi.Ansi
import org.fusesource.jansi.Ansi._
import org.fusesource.jansi.Ansi.Color._
import ch.qos.logback.core.ConsoleAppender
import java.io.File
import java.io.FileOutputStream
import java.io.BufferedOutputStream

object ConsoleManager {
	var appender : ConsoleAppenderWithDeleteSupport = null
	lazy val initAnsi = if (CommandLine.runsInJar) {
	  AnsiConsole.systemInstall();
	} else {
	  disableAnsi
	}
	
	def disableAnsi = Ansi.setEnabled(false)
	def testSetup {
	  disableAnsi
	  new ConsoleAppenderWithDeleteSupport()
	}
}

class ConsoleAppenderWithDeleteSupport extends ConsoleAppender[ILoggingEvent] {
	ConsoleManager.initAnsi
    ConsoleManager.appender = this
  
   @volatile var canDeleteLast = false
  
  override def append(t: ILoggingEvent) {
    if (canDeleteLast) {
      writeSafe("\n")
    }
    super.append(t)
    canDeleteLast = false
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
  
  def sendAnsiLine(message: String) {
    writeSafe(ansi().cursorLeft(100).eraseLine(Erase.FORWARD).a(message).toString)
  } 
    
}