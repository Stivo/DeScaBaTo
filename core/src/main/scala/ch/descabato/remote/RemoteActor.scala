package ch.descabato.remote

import java.io._
import java.util.concurrent.atomic.AtomicLong

import ch.descabato.core_old._
import ch.descabato.frontend.{MaxValueCounter, ProgressReporters, SizeStandardCounter}
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Utils
import com.amazonaws.RequestClientOptions
import com.amazonaws.event.{ProgressEvent, ProgressListener}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest, StorageClass}
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.commons.compress.utils.IOUtils
import org.apache.commons.vfs2.FileObject
import org.apache.commons.vfs2.impl.StandardFileSystemManager

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class NoOpRemoteHandler extends RemoteHandler {
  def startUploading(): Unit = {}

  def stopUploading(): Unit = {}

  def read(backupPath: BackupPath): Future[Unit] = {
    Future.successful(())
  }

  def getFiles(fileType: FileType[_]): List[RemoteFile] = {
    ???
  }

  def load(): Unit = {}

  def shutdown(): BlockingOperation = new BlockingOperation()

  def finish(): Boolean = true

  override def uploadFile(file: File): Unit = {}

  override def fileOperationFinished(operation: RemoteOperation, result: Try[Unit]): Unit = ???

  override def remaining(): Int = 0
}

class SimpleRemoteHandler extends RemoteHandler with Utils {

  var concurrentDownloads = 3
  var concurrentUploads = 1

  private var isUploading = true

  private lazy val remoteClient = RemoteClient.forConfig(config)
  private lazy val remoteOptions = config.remoteOptions

  private var remoteFiles: Map[BackupPath, RemoteFile] = Map.empty

  private var currentUploads: Seq[Upload] = Seq.empty
  private var queuedUploads: Seq[Upload] = Seq.empty
  private var currentDownloads: Seq[Download] = Seq.empty
  private var queuedDownloads: Seq[Download] = Seq.empty

  private var completedUploads = 0L

  override def uploadFile(file: File): Unit = {
    val path = BackupPath(config.relativePath(file))
    val length = file.length()
    uploaderall.maxValue += length
    queuedUploads :+= new Upload(path, length)
    logger.info(s"Queued upload of ${path}")
    queuedUploads = queuedUploads.sortBy(_.size)
    scheduleOperations()
  }

  def startUploading(): Unit = {
    isUploading = true
  }

  def stopUploading(): Unit = {
    isUploading = false
  }

  private def startDownloads(): Unit = {
    while (currentDownloads.size < concurrentDownloads && queuedDownloads.nonEmpty) {
      val next = queuedDownloads.head
      logger.info(s"Scheduling next download ${next.backupPath}")
      Future {
        val dest = localPath(next.backupPath)
        val finished = remoteClient.get(next.backupPath, dest)
        logger.info(s"Download ${next.backupPath} finished with result $finished")
        fileOperationFinishedFromOtherThread(next, finished)
      }
      currentDownloads :+= next
      queuedDownloads = queuedDownloads.tail
    }
  }

  private def startUploads(): Unit = {
    while (currentUploads.size < concurrentUploads && queuedUploads.nonEmpty) {
      val next = queuedUploads.head
      logger.info(s"Starting next upload ${next.backupPath}")
      Future {
        val src = localPath(next.backupPath)
        logger.info(s"Upload ${next.backupPath} started")
        val context = remoteOptions.uploadContext(src.length(), next.backupPath.path)
        val result = remoteClient.put(src, next.backupPath, Some(context), None)
        logger.info(s"Upload ${next.backupPath} finished with result $result")
        fileOperationFinishedFromOtherThread(next, result)
        result
      }
      currentUploads :+= next
      queuedUploads = queuedUploads.tail
    }
  }

  private def localPath(next: BackupPath) = {
    val src = new File(config.folder, next.path)
    src
  }

  private def fileOperationFinishedFromOtherThread(operation: RemoteOperation, result: Try[Unit]): Unit = {
    universe.remoteHandler().fileOperationFinished(operation, result)
  }

  private def scheduleOperations(): Unit = {
    startDownloads()
    startUploads()
  }

  def fileOperationFinished(operation: RemoteOperation, result: Try[Unit]): Unit = {
    result match {
      case Failure(e) => logException(e)
      case _ =>
    }
    operation match {
      case d@Download(_) =>
        currentDownloads = currentDownloads.filterNot(_ == d)
      case u@Upload(_, _) =>
        currentUploads = currentUploads.filterNot(_ == u)
        remoteFiles += u.backupPath -> RemoteFile(u.backupPath, u.size)
        completedUploads += u.size
    }
    scheduleOperations()
    operation.promise.complete(result)
  }

  def read(backupPath: BackupPath): Future[Unit] = {
    val operation = new Download(backupPath)
    queuedDownloads :+= operation
    scheduleOperations()
    operation.promise.future
  }

  def getFiles(fileType: FileType[_]): List[RemoteFile] = {
    ???
  }

  def uploadMissingFiles(): Unit = {
    for (ident <- fileManager.usedIdentifiers) {
      val path = BackupPath(ident)
      if (!remoteFiles.safeContains(path) && path.forConfig(config).exists()) {
        logger.info(s"Uploading file $path")
        uploadFile(path.forConfig(config))
      }
    }
    startUploads()
  }

  def load(): Unit = {
    remoteFiles = remoteClient.list().get.map { rem =>
      (rem.path, rem)
    }.toMap
    deleteFilesWithWrongSizes()
    uploadMissingFiles()
  }

  def deleteFilesWithWrongSizes(): Unit = {
    var toDelete = Seq.empty[(BackupPath, String)]
    for ((path, remoteFile) <- remoteFiles) {
      if (path.path.endsWith(".tmp")) {
        toDelete :+= path -> "it is a temp file"
      }
      val localFile = path.forConfig(config)
      if (localFile.exists()) {
        val length = localFile.length()
        if (length != remoteFile.remoteSize) {
          toDelete :+= path -> s"it has the wrong size (local $length vs remote ${remoteFile.remoteSize})"
        }
      }
    }
    for ((path, reason) <- toDelete) {
      logger.info(s"Deleting remote file $path, because $reason")
      remoteClient.delete(path) match {
        case Failure(x) =>
          logger.warn(s"Could not delete remote file $path", x)
        case _ =>
          remoteFiles -= path
      }
    }
  }

  def shutdown(): BlockingOperation = {
    new BlockingOperation()
  }

  def finish(): Boolean = {
    true
  }

  ProgressReporters.addCounter(new MaxValueCounter {
    override def name: String = "Uploads"

    override def update(): Unit = {
      current = currentUploads.size
      maxValue = currentUploads.size + queuedUploads.size
    }

  })

  private val uploaderall: MaxValueCounter = new SizeStandardCounter("Total Uploads") {
    override def update(): Unit = {
      current = completedUploads + remoteOptions.uploaderCounter1.current
    }
  }
  ProgressReporters.addCounter(uploaderall)

  ProgressReporters.addCounter(new MaxValueCounter {
    override def name: String = "Downloads"

    override def update(): Unit = {
      current = currentDownloads.size
      maxValue = currentDownloads.size + queuedDownloads.size
    }
  })

  override def remaining(): Int = queuedDownloads.size + queuedUploads.size + currentUploads.size + currentDownloads.size
}


class SingleThreadRemoteHandler extends RemoteHandler with Utils {

  private var isUploading = true

  private lazy val remoteClient = RemoteClient.forConfig(config)
  private lazy val mode = config.remoteOptions.mode

  private var remoteFiles: Map[BackupPath, RemoteFile] = Map.empty

  override def uploadFile(file: File): Unit = {
    remoteClient.put(file, BackupPath(config.relativePath(file)))
  }

  def startUploading(): Unit = {
    isUploading = true
  }

  def stopUploading(): Unit = {
    isUploading = false
  }

  private def localPath(next: BackupPath) = {
    val src = new File(config.folder, next.path)
    src
  }

  def fileOperationFinished(operation: RemoteOperation, result: Try[Unit]): Unit = {
  }

  def read(backupPath: BackupPath): Future[Unit] = {
    Future {
      remoteClient.get(backupPath, localPath(backupPath))
    }
  }

  def getFiles(fileType: FileType[_]): List[RemoteFile] = {
    ???
  }

  def load(): Unit = {
    remoteFiles = remoteClient.list().get.map { rem =>
      (rem.path, rem)
    }.toMap
    cleanUpWrongSizeFiles()
  }

  def cleanUpWrongSizeFiles(): Unit = {
  }

  def shutdown(): BlockingOperation = {
    new BlockingOperation()
  }

  def finish(): Boolean = {
    true
  }

  override def remaining(): Int = 0
}


sealed trait RemoteTransferDirection

case object Upload extends RemoteTransferDirection

case object Download extends RemoteTransferDirection

sealed abstract class RemoteOperation(typ: RemoteTransferDirection, backupPath: BackupPath, size: Long) {
  val promise: Promise[Unit] = Promise[Unit]
}

case class Download(backupPath: BackupPath) extends RemoteOperation(Download, backupPath, 0)

case class Upload(backupPath: BackupPath, size: Long) extends RemoteOperation(Upload, backupPath, size)

object RemoteClient {
  def forConfig(config: BackupFolderConfiguration): RemoteClient = {
    val uri = config.remoteOptions.uri
    if (uri.startsWith("s3://")) {
      S3RemoteClient(uri)
    } else if (uri.startsWith("ftp://")) {
      new VfsRemoteClient(uri)
    } else {
      throw new IllegalArgumentException("Could not find implementation for " + uri)
    }
  }
}

trait RemoteClient {
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
      case (s@Success(x), _) => s
      case (f@Failure(_), _) => f
    }
  }

}

object BackupPath {
  private def normalizePath(s: String) = {
    val path = s.replace("\\", "/").replaceAll("/+", "/")
    if (path.startsWith("/")) {
      path.substring(1)
    } else {
      path
    }
  }

  def apply(s: String): BackupPath = {
    new BackupPath(normalizePath(s))
  }
}

class BackupPath private(val path: String) extends AnyVal {
  @JsonIgnore def pathParts: Array[String] = path.split("[\\\\/]")
  @JsonIgnore def name: String = pathParts.last

  def resolve(s: String): BackupPath = {
    BackupPath((path + "/" + s))
  }

  def forConfig(config: BackupFolderConfiguration): File = {
    new File(config.folder, path)
  }

  override def toString: String = path
}

case class RemoteFile(path: BackupPath, remoteSize: Long)

class VfsRemoteClient(url: String) extends RemoteClient with Utils {
  val manager = new StandardFileSystemManager()
  manager.init()
  val remoteDir = manager.resolveFile(url)

  def list(): Try[Seq[RemoteFile]] = {
    Try {
      listIn(BackupPath(""), remoteDir)
    }
  }

  private def listIn(pathSoFar: BackupPath, parent: FileObject): Seq[RemoteFile] = {
    val (folders, files) = parent.getChildren.partition(_.isFolder)
    folders.flatMap { fo =>
      listIn(pathSoFar.resolve(fo.getName.getBaseName), fo)
    } ++ files.map { fo =>
      new RemoteFile(pathSoFar.resolve(fo.getName.getBaseName), fo.getContent.getSize)
    }
  }


  def put(file: File, path: BackupPath, context: Option[RemoteOperationContext], md5Hash: Option[Array[Byte]]): Try[Unit] = {
    val tmpFile = resolvePath(BackupPath(path.path + ".tmp"))
    // return failure if the file already exists
    exists(path).flatMap { exists =>
      Try {
        if (exists) {
          throw new IllegalArgumentException(s"File ${path} already exists")
        }
      }
    }.flatMap { _ =>
      // try to write to temp file
      tryWithResource(new FileInputStream(file)) { in =>
        tryWithResource(tmpFile.getContent.getOutputStream) { out =>
          val throttled = new ThrottlingInputStream(in, context)
          copyStream(throttled, out, None)
        }
      }
    }.flatMap { _ =>
      // try to rename to destination file
      Try {
        val dest = resolvePath(path)
        tmpFile.moveTo(dest)
      }
    }
  }

  def exists(path: BackupPath): Try[Boolean] = {
    Try {
      resolvePath(path).exists()
    }
  }

  def delete(path: BackupPath): Try[Unit] = {
    Try {
      if (!resolvePath(path).delete()) {
        throw new IOException(s"Could not delete file $path")
      }
    }
  }

  def get(path: BackupPath, file: File): Try[Unit] = {
    val fileObject = resolvePath(path)
    tryWithResource(fileObject.getContent.getInputStream) { in =>
      tryWithResource(new FileOutputStream(file)) { out =>
        IOUtils.copy(in, out)
      }
    }
  }

  def getSize(path: BackupPath): Try[Long] = {
    Try {
      resolvePath(path).getContent.getSize
    }
  }

  private def resolvePath(path: BackupPath) = {
    remoteDir.resolveFile(path.path)
  }
}

object S3RemoteClient {
  def apply(url: String): S3RemoteClient = {
    var prefix = url.split("/", 2)(1)
    if (!prefix.endsWith("/")) {
      prefix += "/"
    }
    val bucketName = url.split(":").last.takeWhile(_ != '/')
    new S3RemoteClient(url, bucketName, prefix)
  }
}

class S3RemoteClient private(val url: String, val bucketName: String, val prefix: String) extends RemoteClient {
  lazy val client = AmazonS3ClientBuilder.defaultClient()

  override def get(path: BackupPath, file: File): Try[Unit] = ???

  override def list(): Try[Seq[RemoteFile]] = {
    Try {
      val listing = client.listObjects(bucketName, prefix)
      listing.getObjectSummaries.asScala.map { summary =>
        val path = summary.getKey.replace(prefix, "")
        val size = summary.getSize
        RemoteFile(BackupPath(path), size)
      }
    }
  }

  override def put(file: File, path: BackupPath, context: Option[RemoteOperationContext], md5Hash: Option[Array[Byte]] = None): Try[Unit] = {
    val bufferSize = RequestClientOptions.DEFAULT_STREAM_BUFFER_SIZE
    val remotePath: String = computeRemotePath(path)
    // slow
    //    tryWithResource(new ThrottlingInputStream(new BufferedInputStream(new FileInputStream(file), 2 * bufferSize), context)) { stream =>
    //      val manager = TransferManagerBuilder.standard()
    //      val metadata = new ObjectMetadata()
    //      metadata.setContentLength(file.length)
    //      val upload = manager.upload(bucketName, remotePath, stream, metadata)
    //      upload.waitForUploadResult()
    //    }
    // fast, but no throttling
    Try {
      val manager = TransferManagerBuilder.defaultTransferManager()
      val request = new PutObjectRequest(bucketName, remotePath, file)
      md5Hash.foreach { content =>
        val str = Utils.encodeBase64(content)
        val metadata = new ObjectMetadata()
        metadata.setContentMD5(str)
        request.setMetadata(metadata)
      }
      request.setStorageClass(StorageClass.ReducedRedundancy)
      val upload = manager.upload(request)
      val integer = new AtomicLong()
      context.foreach { c =>
        upload.addProgressListener(new ProgressListener {
          override def progressChanged(progressEvent: ProgressEvent): Unit = {
            val transferred = progressEvent.getBytesTransferred
            val after = integer.addAndGet(transferred)
            c.progress.current = after
          }
        })
      }
      upload.waitForUploadResult()
    }
    // slow
    //    Try {
    //      client.putObject(bucketName, remotePath, file)
    //    }
    //    tryWithResource(new ThrottlingInputStream(new BufferedInputStream(new FileInputStream(file), 2 * bufferSize), context)) { stream =>
    //      val metadata = new ObjectMetadata()
    //      metadata.setContentLength(file.length)
    //      client.putObject(bucketName, remotePath, stream, metadata)
    //    }
  }

  private def computeRemotePath(path: BackupPath) = {
    val remotePath = prefix + path.path
    remotePath
  }

  override def exists(path: BackupPath): Try[Boolean] = ???

  override def delete(path: BackupPath): Try[Unit] = {
    Try {
      client.deleteObject(bucketName, computeRemotePath(path))
    }
  }

  override def getSize(path: BackupPath): Try[Long] = {
    Try {
      ???
    }
  }
}

object RemoteTest extends App {
  private val client = new VfsRemoteClient("ftp://testdescabato:pass1@localhost/")
  private val files: Seq[RemoteFile] = client.list().get
  files.foreach {
    println
  }
  private val file = files(5)
  client.getSize(file.path)
  private val downloaded = new File("downloaded")
  client.get(file.path, downloaded)
  private val path = BackupPath("uploadedagain")
  client.put(downloaded, path)
  println(client.exists(path))
  client.delete(path)
  println(client.exists(path))
  System.exit(1)
}