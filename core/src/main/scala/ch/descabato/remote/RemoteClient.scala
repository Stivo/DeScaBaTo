package ch.descabato.remote

import java.io.Closeable
import java.io.File
import java.io.InputStream
import java.io.OutputStream

import ch.descabato.core.config.BackupFolderConfiguration
import org.apache.commons.compress.utils.IOUtils

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object RemoteClient {
  def forConfig(config: BackupFolderConfiguration): RemoteClient = {
    val uri = config.remoteOptions.uri
    if (uri.startsWith("s3://") || uri.startsWith("arn:aws:s3:::")) {
      S3RemoteClient(uri)
    } else if (uri.startsWith("ftp://")) {
      new VfsRemoteClient(uri)
    } else {
      throw new IllegalArgumentException("Could not find implementation for " + uri)
    }
  }
}

trait RemoteClient extends AutoCloseable {
  def get(path: BackupPath, file: File): Try[Unit]

  def put(file: File, path: BackupPath, context: Option[RemoteOperationContext] = None, md5Hash: Option[Array[Byte]] = None): Try[Unit]

  def exists(path: BackupPath): Try[Boolean]

  def delete(path: BackupPath): Try[Unit]

  def list(): Try[Seq[RemoteFile]]

  def getSize(path: BackupPath): Try[Long]

  protected def copyStream(in: InputStream, out: OutputStream, context: Option[RemoteOperationContext]): Unit = {
    context match {
      case Some(remoteContext) => copyWithProgress(in, out, remoteContext)
      case None => IOUtils.copy(in, out, 64 * 1024)
    }
  }

  protected def copyWithProgress(in: InputStream, out: OutputStream, context: RemoteOperationContext): Unit = {
    val buffer = Array.ofDim[Byte](64 * 1024)
    var lastRead = 0
    while (lastRead >= 0) {
      lastRead = in.read(buffer)
      context.rateLimiter.acquire(lastRead)
      out.write(buffer, 0, lastRead)
      if (lastRead > 0) {
        context.progress.current += lastRead
      }
    }
  }

  def tryWithResource[T, X <: Closeable](resources: => X)(block: X => T): Try[T] = {
    val input = resources
    val result = Try {
      block(input)
    }
    val closeResult = Try {
      input.close()
    }
    (result, closeResult) match {
      case (Success(_), f@Failure(_)) => f.asInstanceOf[Try[T]]
      case (s@Success(_), _) => s
      case (f@Failure(_), _) => f
    }
  }

}