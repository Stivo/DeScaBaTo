package ch.descabato.utils

import java.io.File
import java.util.zip.{ ZipFile => JZipFile }
import java.util.zip.ZipEntry
import java.io.BufferedInputStream
import java.util.zip.ZipOutputStream
import java.io.FileInputStream
import java.io.OutputStream
import ch.descabato.utils.Streams.{DelegatingInputStream, CountingOutputStream, DelegatingOutputStream}
import java.io.InputStream
import java.io.BufferedOutputStream
import java.security.MessageDigest
import java.io.FileOutputStream
import ch.descabato.core.{FileType, BackupFolderConfiguration, FileManager}
import ch.descabato.version.BuildInfo
import java.nio.ByteBuffer
import java.util.zip.CRC32
import ch.descabato.utils.Implicits._
import java.util.zip.Deflater
import scala.collection.mutable

case class MetaInfo(date: String, writingVersion: String)

object ZipFileHandlerFactory {

  def createZipEntry(name: String, header: Byte, content: ByteBuffer) = {
    val ze = new ZipEntry(name)
    ze.setMethod(ZipEntry.STORED)
    ze.setSize(content.remaining()+1)
    ze.setCompressedSize(content.remaining()+1)
    val crc = new CRC32()
    crc.update(header)
    crc.update(content.array(), content.position(), content.remaining())
    ze.setCrc(crc.getValue())
    ze
  }

}

trait ZipFileHandlerCommon {
  def close()
  val file: File
}

trait ZipFileReader extends ZipFileHandlerCommon {

  def names: Seq[String]

  def getStream(name: String): InputStream

  def getEntrySize(name: String): Long

  def getJson[T](name: String)(implicit m: Manifest[T]): Either[T, Exception] = {
    try {
      val in = getStream(name)
      try {
        val js = new JsonSerialization()
        js.readObject(in)
      } finally {
        in.close()
      }
    } catch {
      case e: Exception => Right(e)
    }
  }

  def verifyMd5(hash: Array[Byte]) = {
    val in = new FileInputStream(file)
    val read = new Streams.VerifyInputStream(in, MessageDigest.getInstance("MD5"), hash, file)
    Streams.readFrom(read, (_, _) => Unit)
    // stream is closed in readFrom
  }

}

trait ComplexZipFileWriter extends ZipFileWriter {
  def writeIntoFrom(zipFile: File, relativePath: String = ""): Boolean
  def copyFrom(file: File, path: String, name: String): Boolean
}


trait ZipFileWriter extends ZipFileHandlerCommon {

  def writeEntry(name: String)(f: (OutputStream => Unit)) {
    val bos = newOutputStream(name)
    try {
      f(bos)
    } finally {
      bos.close()
    }
  }

  def writeUncompressedEntry(zipEntry: ZipEntry, byte: Byte, content: ByteBuffer)
  
  def newOutputStream(name: String): OutputStream

  def size(): Long

  def writeJson[T](name: String, t: T)(implicit m: Manifest[T]) {
    writeEntry(name) { o =>
      val js = new JsonSerialization()
      js.writeObject(t, o)
    }
  }

  // TODO manifest in other writer
  def writeManifest(fm: FileManager) {
    val versionNumber: String = BuildInfo.version
    val m = new MetaInfo(fm.getDateFormatted, versionNumber)
    writeJson("manifest.txt", m)
  }

  def enableCompression()
  
  def close()
}

