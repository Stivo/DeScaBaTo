package ch.descabato.rocks.ftp

import java.io.File
import java.nio.file.Paths
import java.util.Objects

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.rocks.RocksEnv
import jnr.ffi.Platform
import jnr.ffi.Platform.OS
import jnr.ffi.Pointer
import jnr.ffi.types.off_t
import jnr.ffi.types.size_t
import org.apache.commons.compress.utils.IOUtils
import ru.serce.jnrfuse.ErrorCodes
import ru.serce.jnrfuse.FuseFillDir
import ru.serce.jnrfuse.FuseStubFS
import ru.serce.jnrfuse.struct.FileStat
import ru.serce.jnrfuse.struct.FuseFileInfo

object TestFuse {

  def main(args: Array[String]) = {
    val rootFolder = "l:/backup_zstd_9"

    val config = BackupFolderConfiguration(new File(rootFolder))
    val env = RocksEnv(config, readOnly = true)
    val reader = new BackupReader(env)
    val stub = new TestFuse(reader)
    try {
      val path = Platform.getNativePlatform.getOS match {
        case OS.WINDOWS =>
          "J:\\"
        case _ =>
          "/tmp/mnth"
      }
      stub.mount(Paths.get(path), true, true)
    } finally stub.umount()
  }
}

class TestFuse(reader: BackupReader) extends FuseStubFS {
  val readOnly = 0x124 // 0x444
  val readWriteAndList = 0x1ed // 0x755
  val readAndList = 0x16d // 0x555

  def lookup(pathIn: String): Option[TreeNode] = {
    if (pathIn == "/") {
      Some(reader.rootDirectory)
    } else {
      val path = PathUtils.splitPath(pathIn).tail
      val out = reader.rootDirectory.find(path)
      //      println(s"In: $pathIn, path: ${path}, out: ${out}")
      out
    }
  }

  override def getattr(path: String, stat: FileStat) = {
    var res = 0
    if (Objects.equals(path, "/")) {
      stat.st_mode.set(FileStat.S_IFDIR | readAndList)
      stat.st_nlink.set(2)
    } else {
      val memoryPath = lookup(path)
      memoryPath match {
        case Some(x: FolderNode) =>
          stat.st_mode.set(FileStat.S_IFDIR | readAndList)
          stat.st_nlink.set(2)
        case Some(x: FileNode) =>
          stat.st_mode.set(FileStat.S_IFREG | readOnly)
          stat.st_nlink.set(1)
          stat.st_size.set(x.metadata.length)
        case None =>
          res = -ErrorCodes.ENOENT()
      }
    }
    res
  }

  override def readdir(path: String, buf: Pointer, filter: FuseFillDir, @off_t offset: Long, fi: FuseFileInfo): Int = {
    lookup(path) match {
      case Some(folderNode: FolderNode) =>
        filter.apply(buf, ".", null, 0)
        filter.apply(buf, "..", null, 0)
        folderNode.children.foreach { ff =>
          filter.apply(buf, ff.name, null, 0)
        }
        0
      case None =>
        -ErrorCodes.ENOENT()
    }
  }

  override def open(path: String, fi: FuseFileInfo): Int = {
    lookup(path) match {
      case Some(x: FileNode) =>
        0
      case _ =>
        -ErrorCodes.ENOENT()
    }
  }

  override def read(path: String, buf: Pointer, @size_t sizeIn: Long, @off_t offset: Long, fi: FuseFileInfo): Int = {
    lookup(path) match {
      case Some(x: FileNode) =>
        val input = reader.createInputStream(x.metadata)
        input.skip(offset)
        val bytes = Array.ofDim[Byte](sizeIn.toInt)
        val read = IOUtils.readFully(input, bytes)
        buf.put(0, bytes, 0, read)
        println(s"Read from $path at $offset with length $sizeIn, returned $read bytes")
        read
      case _ =>
        -ErrorCodes.ENOENT()
    }
  }
}