package ch.descabato.remote

import ch.descabato.RemoteMode
import ch.descabato.frontend.{FileCounter, ProgressReporters}
import com.google.common.util.concurrent.RateLimiter

class RemoteOptions {

  var uri: String = _
  var mode: RemoteMode = RemoteMode.NoRemote

  @transient
  def enabled: Boolean = uri != null

  // set to 1 GB for unlimited initially
  private var _uploadSpeedLimitKiloBytesPerSecond: Int = 1024 * 1024

  @transient
  lazy val uploadRateLimiter: RateLimiter = RateLimiter.create(_uploadSpeedLimitKiloBytesPerSecond * 1024)

  def setUploadSpeedLimitKiloBytesPerSecond(newRate: Int): Unit = {
    _uploadSpeedLimitKiloBytesPerSecond = newRate
    uploadRateLimiter.setRate(newRate * 1024)
  }

  def getUploadSpeedLimitKiloBytesPerSecond(): Int = _uploadSpeedLimitKiloBytesPerSecond

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

  @transient
  lazy val uploaderCounter1: FileCounter = new FileCounter {
    ProgressReporters.addCounter(this)
  }

}

case class RemoteOperationContext(rateLimiter: RateLimiter, progress: FileCounter) {
  def initCounter(fileSize: Long, fileName: String) = {
    progress.resetSnapshots()
    progress.fileName = fileName
    progress.maxValue = fileSize
    progress.current = 0
  }

}