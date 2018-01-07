package ch.descabato.remote

import java.io._

import ch.descabato.core._
import ch.descabato.frontend.{FileCounter, MaxValueCounter, ProgressReporters, SizeStandardCounter}
import ch.descabato.utils.Utils
import ch.descabato.utils.Implicits._
import org.apache.commons.compress.utils.IOUtils
import org.apache.commons.vfs2.FileObject
import org.apache.commons.vfs2.impl.StandardFileSystemManager

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
  private lazy val mode = config.remoteMode

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
      val future = Future {
        val src = localPath(next.backupPath)
        val result = remoteClient.put(src, next.backupPath, Some(uploader1))
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

  private val uploader1: FileCounter = new FileCounter {
    override def name: String = "Uploader 1"
  }
  ProgressReporters.addCounter(uploader1)

  private val uploaderall: MaxValueCounter = new SizeStandardCounter {
    override def name: String = "Total Uploads"

    override def update(): Unit = {
      current = completedUploads + uploader1.current
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
  private lazy val mode = config.remoteMode

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
    if (config.remoteUri.startsWith("ftp://")) {
      new VfsRemoteClient(config.remoteUri)
    } else {
      throw new IllegalArgumentException("Could not find implementation for " + config.remoteUri)
    }
  }
}

trait RemoteClient {
  def get(path: BackupPath, file: File): Try[Unit]

  def put(file: File, path: BackupPath, counter: Option[MaxValueCounter] = None): Try[Unit]

  def exists(path: BackupPath): Try[Boolean]

  def delete(path: BackupPath): Try[Unit]

  def list(): Try[Seq[RemoteFile]]

  def getSize(path: BackupPath): Try[Long]

  protected def copyStream(in: FileInputStream, out: OutputStream, progressCounter: Option[MaxValueCounter], size: Long, filename: String): Unit = {
    progressCounter match {
      case Some(pc) => copyWithProgress(in, out, pc, size, filename)
      case None => IOUtils.copy(in, out, 64 * 1024)
    }
  }

  protected def copyWithProgress(in: FileInputStream, out: OutputStream, pc: MaxValueCounter, size: Long, filename: String): Unit = {
    pc.maxValue = size
    pc match {
      case x: FileCounter => x.fileName = filename
      case _ =>
    }
    val buffer = Array.ofDim[Byte](64 * 1024)
    var lastRead = 0
    var count = 0L
    while (lastRead >= 0) {
      lastRead = in.read(buffer)
      out.write(buffer, 0, lastRead)
      if (lastRead > 0) {
        count += lastRead
        pc.current = count
      }
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


  def put(file: File, path: BackupPath, counter: Option[MaxValueCounter]): Try[Unit] = {
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
          copyStream(in, out, counter, file.length(), file.getName)
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