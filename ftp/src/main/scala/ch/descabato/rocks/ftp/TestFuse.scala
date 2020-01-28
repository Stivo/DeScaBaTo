package ch.descabato.rocks.ftp

import java.nio.file.Paths
import java.util.Objects

import jnr.ffi.Platform
import jnr.ffi.Platform.OS
import jnr.ffi.Pointer
import jnr.ffi.types.off_t
import jnr.ffi.types.size_t
import ru.serce.jnrfuse.ErrorCodes
import ru.serce.jnrfuse.FuseFillDir
import ru.serce.jnrfuse.FuseStubFS
import ru.serce.jnrfuse.struct.FileStat
import ru.serce.jnrfuse.struct.FuseFileInfo

object TestFuse {
  val HELLO_PATH = "/hello"
  val HELLO_STR = "Hello World!"

  def main(args: Array[String]) = {
    val stub = new TestFuse
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

class TestFuse extends FuseStubFS {
  override def getattr(path: String, stat: FileStat) = {
    var res = 0
    if (Objects.equals(path, "/")) {
      stat.st_mode.set(FileStat.S_IFDIR | 0x1ed)
      stat.st_nlink.set(2)
    }
    else if (TestFuse.HELLO_PATH == path) {
      stat.st_mode.set(FileStat.S_IFREG | 0x124)
      stat.st_nlink.set(1)
      stat.st_size.set(TestFuse.HELLO_STR.getBytes.length)
    }
    else res = -ErrorCodes.ENOENT
    res
  }

  override def readdir(path: String, buf: Pointer, filter: FuseFillDir, @off_t offset: Long, fi: FuseFileInfo): Int = {
    if (!("/" == path)) {
      -ErrorCodes.ENOENT
    } else {
      filter.apply(buf, ".", null, 0)
      filter.apply(buf, "..", null, 0)
      filter.apply(buf, TestFuse.HELLO_PATH.substring(1), null, 0)
      0
    }
  }

  override def open(path: String, fi: FuseFileInfo): Int = {
    if (!(TestFuse.HELLO_PATH == path)) {
      -ErrorCodes.ENOENT
    } else {
      0
    }
  }

  override def read(path: String, buf: Pointer, @size_t sizeIn: Long, @off_t offset: Long, fi: FuseFileInfo): Int = {
    if (!(TestFuse.HELLO_PATH == path)) {
      -ErrorCodes.ENOENT
    } else {
      var size = sizeIn
      val bytes = TestFuse.HELLO_STR.getBytes
      val length = bytes.length
      if (offset < length) {
        if (offset + size > length) {
          size = length - offset
        }
        buf.put(0, bytes, 0, bytes.length)
      } else {
        size = 0
      }
      size.toInt
    }
  }
}