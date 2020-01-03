package ch.descabato.remote

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException

import ch.descabato.utils.Utils
import org.apache.commons.compress.utils.IOUtils
import org.apache.commons.vfs2.FileObject
import org.apache.commons.vfs2.impl.StandardFileSystemManager

import scala.util.Try

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

  override def close(): Unit = {
    manager.close()
  }
}
