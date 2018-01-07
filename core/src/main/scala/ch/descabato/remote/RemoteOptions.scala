package ch.descabato.remote

import ch.descabato.RemoteMode
import ch.descabato.frontend.{FileCounter, ProgressReporters}
import com.google.common.util.concurrent.RateLimiter

class RemoteOptions {


  var uri: String = null
  var mode: RemoteMode = RemoteMode.VolumesOnBoth

  def enabled: Boolean = uri != null

  // set to 1 GB for unlimited initially
  private var _uploadSpeedLimitKiloBytesPerSecond: Int = 1024 * 1024

  val uploadRateLimiter: RateLimiter = RateLimiter.create(_uploadSpeedLimitKiloBytesPerSecond * 1024)

  def uploadSpeedLimitKiloBytesPerSecond_=(newRate: Int): Unit = {
    _uploadSpeedLimitKiloBytesPerSecond = newRate
    uploadRateLimiter.setRate(newRate * 1024)
  }

  def uploadContext(fileSize: Long, fileName: String): RemoteOperationContext = {
    val out = new RemoteOperationContext(uploadRateLimiter, uploaderCounter1)
    out.initCounter(fileSize, fileName)
    out
  }

  def verify(): Unit = {
    if (uri != null && mode == RemoteMode.NoRemote) {
      throw new IllegalArgumentException(s"RemoteMode can not be ${RemoteMode.NoRemote} if a remote URI has been set")
    }
    if (mode != RemoteMode.NoRemote && uri == null) {
      throw new IllegalArgumentException(s"RemoteMode is set to ${mode} but no remote URI has been set")
    }
  }
  lazy val uploaderCounter1: FileCounter = new FileCounter {
    override def name: String = "Uploader 1"
    ProgressReporters.addCounter(this)
  }

}

case class RemoteOperationContext(rateLimiter: RateLimiter, progress: FileCounter) {
  def initCounter(fileSize: Long, fileName: String) = {
    progress.fileName = fileName
    progress.maxValue = fileSize
    progress.current = 0
  }

}