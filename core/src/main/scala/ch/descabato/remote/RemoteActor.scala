package ch.descabato.remote

import java.io._

import akka.actor.{ActorRef, TypedActor}
import ch.descabato.core.LifeCycle
import ch.descabato.core.actors._
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.util.JacksonAnnotations.JsonIgnore
import ch.descabato.frontend.{MaxValueCounter, ProgressReporters, SizeStandardCounter}
import ch.descabato.utils.Implicits._
import ch.descabato.utils.{Hash, Utils}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Try}


trait RemoteHandler extends MyEventReceiver with LifeCycle {
  var remoteContext: Option[RemoteOperationContext] = None

  protected var isUploading = true

  def stopUploading(): Unit = {
    isUploading = false
  }

  def startUploading(): Unit = {
    isUploading = true
  }

  def remainingFiles(): Int

  def remainingBytes(): Long
}

class SimpleRemoteHandler(backupContext: BackupContext, journalHandler: JournalHandler) extends RemoteHandler with Utils with TypedActor.Receiver {

  var concurrentDownloads = 3
  var concurrentUploads = 1

  val config = backupContext.config

  private lazy val remoteClient = RemoteClient.forConfig(config)
  private lazy val remoteOptions = config.remoteOptions

  private var remoteFiles: Map[BackupPath, RemoteFile] = Map.empty

  private var currentUploads: Seq[Upload] = Seq.empty
  private var queuedUploads: Seq[Upload] = Seq.empty
  private var currentDownloads: Seq[Download] = Seq.empty
  private var queuedDownloads: Seq[Download] = Seq.empty

  private var completedUploads = 0L

  private def queueUpload(file: File, md5hash: Hash): Unit = {
    val path = BackupPath(config.relativePath(file))
    val length = file.length()
    uploaderall.maxValue += length
    queuedUploads :+= new Upload(path, length, md5hash)
    logger.info(s"Queued upload of ${path}")
    scheduleOperations()
  }

  //  private def startDownloads(): Unit = {
  //    while (currentDownloads.size < concurrentDownloads && queuedDownloads.nonEmpty) {
  //      val next = queuedDownloads.head
  //      logger.info(s"Scheduling next download ${next.backupPath}")
  //      Future {
  //        val dest = localPath(next.backupPath)
  //        val finished = remoteClient.get(next.backupPath, dest)
  //        logger.info(s"Download ${next.backupPath} finished with result $finished")
  //        fileOperationFinishedFromOtherThread(next, finished)
  //      }
  //      currentDownloads :+= next
  //      queuedDownloads = queuedDownloads.tail
  //    }
  //  }

  var ownActorRef: Option[ActorRef] = None

  private def startNextUpload(): Unit = {
    require(ownActorRef.isDefined)
    queuedUploads = queuedUploads.sortBy(x => (x.failureCounter, x.size))
    while (currentUploads.size < concurrentUploads && queuedUploads.nonEmpty && isUploading) {
      val next = queuedUploads.head
      if (next.failureCounter >= 10) {
        logger.warn(s"Aborting upload of ${next.backupPath} due to too many failures")
        queuedUploads = queuedUploads.tail
      } else {
        logger.info(s"Starting next upload ${next.backupPath}")
        Future {
          val src = localPath(next.backupPath)
          logger.info(s"Upload ${next.backupPath} started")
          val context = remoteOptions.uploadContext(src.length(), next.backupPath.path)
          val result = remoteClient.put(src, next.backupPath, Some(context), Some(next.md5Hash))
          logger.info(s"Upload ${next.backupPath} finished with result $result")
          ownActorRef.get ! FileOperationFinished(next, result)
          result
        }
        currentUploads :+= next
        queuedUploads = queuedUploads.tail
      }
    }
  }

  override def startUploading(): Unit = {
    super.startUploading()
    startNextUpload()
  }

  private def localPath(next: BackupPath) = {
    new File(config.folder, next.path)
  }

  override def onReceive(message: Any, sender: ActorRef): Unit = {
    message match {
      case FileOperationFinished(next, result) =>
        fileOperationFinished(next, result)
      case x =>
        logger.info(s"Got unknown message $x")
    }
  }

  override def receive(myEvent: MyEvent): Unit = {
    myEvent match {
      case FileAddedToJournal(_, file, md5hash) =>
        queueUpload(file, md5hash)
      case _ =>
      // ignore other events
    }
  }

  private def scheduleOperations(): Unit = {
    startNextUpload()
  }

  def fileOperationFinished(operation: RemoteOperation, result: Try[Unit]): Unit = {
    result match {
      case Failure(e) => logException(e)
      case _ =>
    }
    operation match {
      case d@Download(_) =>
        currentDownloads = currentDownloads.filterNot(_ == d)
      case u@Upload(_, _, _) =>
        currentUploads = currentUploads.filterNot(_ == u)
        if (result.isSuccess) {
          logger.info(s"Upload of ${u.backupPath} has finished")
          remoteFiles += u.backupPath -> RemoteFile(u.backupPath, u.size)
          completedUploads += u.size
        } else {
          logger.info(s"Upload of ${u.backupPath} has failed for the ${u.failureCounter} time, will retry later")
          queuedUploads :+= u
          u.failureCounter += 1
          Thread.sleep(5000)
        }
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

  def uploadMissingFiles(): Unit = {
    for (ident <- backupContext.fileManager.allFiles()) {
      val path = BackupPath(ident, backupContext.config)
      if (!remoteFiles.safeContains(path) && path.forConfig(config).exists()) {
        logger.info(s"Uploading file $path")
        queueUpload(path.forConfig(config), journalHandler.getMd5ForFile(ident).get)
      }
    }
    startNextUpload()
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

  def finish(): Future[Boolean] = {
    val remaining = remainingFiles()
    Future.successful(remaining == 0)
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
      val before = current
      this += (completedUploads + remoteOptions.uploaderCounter1.current) - before
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

  override def remainingFiles(): Int = queuedDownloads.size + queuedUploads.size + currentUploads.size + currentDownloads.size

  override def remainingBytes(): Long = (queuedUploads ++ currentUploads).map(_.size).sum

  override def startup(): Future[Boolean] = {
    logger.info("Startup called")
    ownActorRef = Some(TypedActor(TypedActor.context).getActorRefFor(TypedActor.self[RemoteHandler]))
    load()
    Future.successful(true)
  }
}

sealed trait RemoteTransferDirection

case object Upload extends RemoteTransferDirection

case object Download extends RemoteTransferDirection

sealed abstract class RemoteOperation(typ: RemoteTransferDirection, backupPath: BackupPath, size: Long) {
  val promise: Promise[Unit] = Promise[Unit]

  var failureCounter = 0
}

case class Download(backupPath: BackupPath) extends RemoteOperation(Download, backupPath, 0)

case class Upload(backupPath: BackupPath, size: Long, md5Hash: Hash) extends RemoteOperation(Upload, backupPath, size)


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

  def apply(f: File, config: BackupFolderConfiguration): BackupPath = {
    new BackupPath(normalizePath(config.folder.toPath.relativize(f.toPath).toString))
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

case class FileOperationFinished(next: Upload, result: Try[Unit])