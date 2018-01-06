package ch.descabato.remote

import java.io.{File, FileInputStream, FileOutputStream}

import ch.descabato.core.UniversePart
import org.apache.commons.compress.utils.IOUtils
import org.apache.commons.vfs2.FileObject
import org.apache.commons.vfs2.impl.StandardFileSystemManager

class RemoteActor extends UniversePart {

}

trait RemoteClient {
  def get(path: BackupPath, file: File)

  def put(file: File, path: BackupPath)

  def exists(path: BackupPath): Boolean

  def delete(path: BackupPath)

  def list(): Seq[RemoteFile]

  def getSize(path: BackupPath): Long
}

class BackupPath(val path: String) extends AnyVal {
  def resolve(s: String): BackupPath = {
    new BackupPath((path + "/" + s).replace('\\', '/').replaceAll("//", "/"))
  }

  override def toString: String = path
}

case class RemoteFile(path: BackupPath, remoteSize: Long)

class VfsRemoteClient(url: String) extends RemoteClient {
  val manager = new StandardFileSystemManager()
  manager.init()
  val remoteDir = manager.resolveFile(url)

  def list(): Seq[RemoteFile] = {
    listIn(new BackupPath(""), remoteDir)
  }

  private def listIn(pathSoFar: BackupPath, parent: FileObject): Seq[RemoteFile] = {
    val (folders, files) = parent.getChildren.partition(_.isFolder)
    folders.flatMap { fo =>
      listIn(pathSoFar.resolve(fo.getName.getBaseName), fo)
    } ++ files.map { fo =>
      new RemoteFile(pathSoFar.resolve(fo.getName.getBaseName), fo.getContent.getSize)
    }
  }

  def put(file: File, path: BackupPath) = {
    val dest = resolvePath(path)
    val in = new FileInputStream(file)
    val out = dest.getContent.getOutputStream
    IOUtils.copy(in, out)
    IOUtils.closeQuietly(in)
    IOUtils.closeQuietly(out)
  }

  def exists(path: BackupPath): Boolean = {
    resolvePath(path).exists()
  }

  def delete(path: BackupPath) = {
    resolvePath(path).delete()
  }

  def get(path: BackupPath, file: File): Unit = {
    val fileObject = resolvePath(path)
    val in = fileObject.getContent.getInputStream
    val out = new FileOutputStream(file)
    IOUtils.copy(in, out)
    IOUtils.closeQuietly(in)
    IOUtils.closeQuietly(out)
  }

  def getSize(path: BackupPath): Long = {
    resolvePath(path).getContent.getSize
  }

  private def resolvePath(path: BackupPath) = {
    remoteDir.resolveFile(path.path)
  }
}


object RemoteTest extends App {
  private val client = new VfsRemoteClient("ftp://testdescabato:pass1@localhost/")
  private val files: Seq[RemoteFile] = client.list()
  files.foreach {
    println
  }
  private val file = files(5)
  client.getSize(file.path)
  private val downloaded = new File("downloaded")
  client.get(file.path, downloaded)
  private val path = new BackupPath("uploadedagain")
  client.put(downloaded, path)
  println(client.exists(path))
  client.delete(path)
  println(client.exists(path))
  System.exit(1)
}