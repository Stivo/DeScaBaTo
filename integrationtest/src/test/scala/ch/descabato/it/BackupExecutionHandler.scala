package ch.descabato.it

import java.io.{File, FileOutputStream}

import ch.descabato.utils.Streams.DelegatingOutputStream
import ch.descabato.utils.Utils
import org.apache.commons.exec._
import org.apache.commons.exec.environment.EnvironmentUtils

class BackupExecutionHandler(args: CommandLine, logfolder: File, name: String,
                             val secs: Int = 600)
  extends ExecuteWatchdog(secs * 1000) with ExecuteResultHandler with Utils {
  private val executor = new DefaultExecutor()
  executor.setWatchdog(this)
  executor.setWorkingDirectory(logfolder)
  val out = new FileOutputStream(new File(logfolder, name + "_out.log"), true)
  val error = new FileOutputStream(new File(logfolder, name + "_error.log"), true)
  val streamHandler = new PumpStreamHandler(new DelegatingOutputStream(out, System.out),
    new DelegatingOutputStream(error, System.err))
  executor.setStreamHandler(streamHandler)
  @volatile var finished = false
  var exit = Int.MinValue

  def onProcessComplete(exitValue: Int) {
    finished = true
    exit = exitValue
    close()
  }

  override def destroyProcess() {
    super.destroyProcess()
    close()
  }

  def onProcessFailed(e: ExecuteException) {
    logException(e)
    finished = true
    exit = -1
    close()
  }

  val baseFolder = logfolder.getParentFile().getParentFile()
  //val jacoco = new File(baseFolder, "jacocoagent.jar")
  //val destfile = new File(baseFolder, "core/target/scala-2.10/jacoco/jacoco.exec")
  //jacoco.exists() should be
  val map = EnvironmentUtils.getProcEnvironment()
  //EnvironmentUtils.addVariableToEnvironment(map, s"JVM_OPT=-javaagent:$jacoco=destfile=$destfile,append=true")

  def startAndWait() = {
    val out = executor.execute(args, map)
    close()
    out
  }

  def start() {
    executor.execute(args, map, this)
  }

  def close() {
    out.close()
    error.close()
  }

}
