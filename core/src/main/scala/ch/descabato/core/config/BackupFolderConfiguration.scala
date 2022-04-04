package ch.descabato.core.config

import ch.descabato.CompressionMode
import ch.descabato.HashAlgorithm
import ch.descabato.core.model.Size
import ch.descabato.core.util.JacksonAnnotations.JsonIgnore
import ch.descabato.core.util._
import ch.descabato.remote.RemoteOptions
import ch.descabato.utils.JsonSerialization
import com.github.luben.zstd.ZstdInputStream
import com.github.luben.zstd.ZstdOutputStream
import org.bouncycastle.crypto.Digest

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.File
import java.io.InputStream
import java.io.OutputStream

case class BackupFolderConfiguration(folder: File, @JsonIgnore var passphrase: Option[String] = None, newBackup: Boolean = false) {

  def this() = this(null)

  @JsonIgnore
  var configFileName: String = "backup.json"
  var version: String = ch.descabato.utils.BuildInfo.version

  var keyLength = 128
  var compressor = CompressionMode.smart

  var hashAlgorithm: HashAlgorithm = HashAlgorithm.sha3_256

  @JsonIgnore def createMessageDigest(): Digest = hashAlgorithm.newInstance()

  var volumeSize: Size = Size("100Mb")
  var threads: Int = 1
  //  val useDeltas = false
  var hasPassword: Boolean = passphrase.isDefined
  //  var renameDetection = true
  //  var redundancyEnabled = false
  //  var metadataRedundancy: Int = 20
  //  var volumeRedundancy: Int = 5
  var saveSymlinks: Boolean = true
  var ignoreFile: Option[File] = None

  var remoteOptions: RemoteOptions = new RemoteOptions()

  var key: Array[Byte] = _

  def newWriter(file: File): FileWriter = {
    if (passphrase.isEmpty) {
      new SimpleFileWriter(file)
    } else {
      new EncryptedFileWriter(file, passphrase.get, keyLength)
    }
  }

  def newReader(file: File): FileReader = {
    if (passphrase.isEmpty) {
      new SimpleFileReader(file)
    } else {
      new EncryptedFileReader(file, passphrase.get)
    }
  }

  def newCompressedOutputStream(file: File): OutputStream = {
    new ZstdOutputStream(new BufferedOutputStream(newWriter(file).asOutputStream(), 10 * 1024 * 1024), 7)
  }

  def newCompressedInputStream(file: File): InputStream = {
    new ZstdInputStream(new BufferedInputStream(newReader(file).asInputStream(), 10 * 1024 * 1024))
  }

  def relativePath(file: File): String = {
    folder.toPath.relativize(file.toPath).toString.replace('\\', '/')
  }

  def resolveRelativePath(file: String): File = {
    folder.toPath.resolve(file).toFile
  }

  def verify(): Unit = {
    remoteOptions.verify()
  }

  @JsonIgnore
  def asJson(): Array[Byte] = {
    val json = new JsonSerialization()
    json.write(this)
  }

}
