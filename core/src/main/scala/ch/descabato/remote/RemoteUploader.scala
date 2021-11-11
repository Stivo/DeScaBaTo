package ch.descabato.remote

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.BackupEnv
import ch.descabato.core.model.ValueLogStatusKey

import java.io.File
import java.util.concurrent.Executors
import ch.descabato.rocks.protobuf.keys.Status
import ch.descabato.utils.Hash
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Utils

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutorService
import scala.concurrent.Future
import scala.concurrent.duration.Duration

class RemoteUploader(rocksEnv: BackupEnv) extends Utils with AutoCloseable {
  private var remoteFiles: Map[BackupPath, RemoteFile] = Map.empty
  val ex: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))

  val remoteClient: RemoteClient = RemoteClient.forConfig(rocksEnv.config)
  val config: BackupFolderConfiguration = rocksEnv.config

  private def localPath(next: BackupPath) = {
    new File(config.folder, next.path)
  }

  private var queuedUploads: Seq[Upload] = Seq.empty

  def upload(head: Upload): Unit = {
    val src = localPath(head.backupPath)
    logger.info(s"Upload ${head.backupPath} started")
    //    val context = remoteOptions.uploadContext(src.length(), next.backupPath.path)
    val result = remoteClient.put(src, head.backupPath, None, Some(head.md5Hash))
    logger.info(s"Upload ${head.backupPath} finished with result $result")
    //    ownActorRef.get ! FileOperationFinished(next, result)
    result
  }

  def uploadAllRemaining(maxUploads: Int): (Int, Int) = {
    queuedUploads = Seq.empty
    remoteFiles = remoteClient.list().get.map { rem =>
      (rem.path, rem)
    }.toMap
    for (ident <- rocksEnv.fileManager.allFiles()) {
      val key = ValueLogStatusKey(rocksEnv.config.relativePath(ident))
      val value = rocksEnv.rocks.readValueLogStatus(key)
      if (value.exists(_.status == Status.FINISHED)) {
        val path = BackupPath(ident, config)
        if (!remoteFiles.safeContains(path) && path.forConfig(config).exists()) {
          logger.info(s"Uploading file $path")
          queueUpload(path.forConfig(config), Hash(value.get.md5Hash.toByteArray))
        }
      }
    }
    uploadSomeFiles(maxUploads)
  }

  private def uploadSomeFiles(maxUploads: Int): (Int, Int) = {
    val futures = for (fileToUpload <- queuedUploads.take(maxUploads)) yield {
      val fut = Future {
        upload(fileToUpload)
      }(ex)
      fut
    }
    var finished = 0
    var failed = 0
    for (future <- futures) {
      Await.ready(future, Duration.Inf)
      if (future.value.get.isSuccess) {
        finished += 1
      } else if (future.value.isEmpty) {
        failed += 1
      }
    }
    (finished, failed)
  }

  private def queueUpload(file: File, md5hash: Hash): Unit = {
    val path = BackupPath(config.relativePath(file))
    val length = file.length()
    //    uploaderall.maxValue += length // TODO
    queuedUploads :+= new Upload(path, length, md5hash)
    logger.info(s"Queued upload of ${path}")
  }

  override def close(): Unit = {
    ex.shutdown()
    remoteClient.close()
  }
}
