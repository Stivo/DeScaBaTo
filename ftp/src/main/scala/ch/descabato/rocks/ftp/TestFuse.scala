package ch.descabato.rocks.ftp

import java.io.File
import java.io.InputStream
import java.nio.file.Paths

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.rocks.RocksEnv
import ch.descabato.utils.Utils
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
      stub.mount(Paths.get(path), true, false)
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
    val memoryPath = lookup(path)
    memoryPath match {
      case Some(x: FileFolder) =>
        stat.st_mode.set(FileStat.S_IFDIR | readAndList)
        stat.st_nlink.set(2)
      case Some(x: FileNode) =>
        stat.st_mode.set(FileStat.S_IFREG | readOnly)
        stat.st_nlink.set(1)
        stat.st_size.set(x.metadata.length)
        stat.st_mtim.tv_sec.set(x.fileMetadataKeyWrapper.fileMetadataKey.changed / 1000)
        val createdTime = if (path.startsWith("/" + PathUtils.allFilesPath)) {
          val hour = 1000L * 60 * 60
          12 * hour + (x.linkCount - 1) * 24 * hour
        } else {
          x.metadata.created / 1000
        }
        stat.st_ctim.tv_sec.set(createdTime)
        val totalSize = reader.rocksEnv.rocks.getIndexes(x.metadata).map(_.lengthCompressed.toLong).sum
        println(s"Total size for $path is ${Utils.readableFileSize(x.metadata.length)}, but with compression: ${Utils.readableFileSize(totalSize)}. ")
      case None =>
        res = -ErrorCodes.ENOENT()
    }
    res
  }

  override def getxattr(path: String, name: String, value: Pointer, size: Long): Int = {
    println(s"Getting extended attribute $name for $path with size $size")
    super.getxattr(path, name, value, size)
  }

  override def listxattr(path: String, list: Pointer, size: Long): Int = {
    println(s"Listing extended attributes for $path")
    super.listxattr(path, list, size)
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
      case Some(_: FileNode) =>
        0
      case _ =>
        -ErrorCodes.ENOENT()
    }
  }

  var cache: Map[CacheEntry, InputStream] = Map.empty

  case class CacheEntry(path: String, offset: Long)

  override def read(path: String, buf: Pointer, @size_t sizeIn: Long, @off_t offset: Long, fi: FuseFileInfo): Int = {
    lookup(path) match {
      case Some(x: FileNode) =>
        this.synchronized {
          val entry = CacheEntry(path, offset)
          val input = cache.get(entry) match {
            case Some(x) =>
              cache -= entry
              println(s"Cache hit on $entry")
              x
            case None =>
              val input = reader.createInputStream(x.metadata)
              input.skip(offset)
              input
          }
          val bytes = Array.ofDim[Byte](sizeIn.toInt)
          val read = IOUtils.readFully(input, bytes)
          buf.put(0, bytes, 0, read)
          println(s"Read from $path at $offset with length $sizeIn, returned $read bytes")
          if (offset + read < x.metadata.length) {
            val entry1 = entry.copy(offset = offset + read)
            println(s"Added cache entry for $entry1")
            cache += entry1 -> input
          }
          read
        }
      case _ =>
        -ErrorCodes.ENOENT()
    }
  }
}