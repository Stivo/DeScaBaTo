package ch.descabato.remote

import java.io._

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.util.JacksonAnnotations.JsonIgnore
import ch.descabato.utils.Hash

import scala.concurrent.Promise
import scala.util.Try


sealed trait RemoteTransferDirection

case object Upload extends RemoteTransferDirection

case object Download extends RemoteTransferDirection

sealed abstract class RemoteOperation(typ: RemoteTransferDirection, backupPath: BackupPath, size: Long) {
  var promise: Promise[Unit] = Promise[Unit]

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

class S3RemoteFile(path: BackupPath, remoteSize: Long, val etag: String) extends RemoteFile(path, remoteSize) {
  override def toString: String = super.toString + s", ETag: ${etag}"
}

//object RemoteTest extends App {
//  private val client = new VfsRemoteClient("ftp://testdescabato:pass1@localhost/")
//  private val files: Seq[RemoteFile] = client.list().get
//  files.foreach {
//    println
//  }
//  private val file = files(5)
//  client.getSize(file.path)
//  private val downloaded = new File("downloaded")
//  client.get(file.path, downloaded)
//  private val path = BackupPath("uploadedagain")
//  client.put(downloaded, path)
//  println(client.exists(path))
//  client.delete(path)
//  println(client.exists(path))
//  System.exit(1)
//}

case class FileOperationFinished(next: Upload, result: Try[Unit])