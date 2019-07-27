package ch.descabato.remote

import java.io.{BufferedInputStream, File, FileInputStream, InputStream}

import ch.descabato.utils.{Hash, Utils}
import com.amazonaws.RequestClientOptions
import com.amazonaws.event.{ProgressEvent, ProgressEventType, ProgressListener}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import org.apache.commons.compress.utils.BoundedInputStream

import scala.collection.JavaConverters._
import scala.util.Try

object S3RemoteClient {
  def apply(url: String): S3RemoteClient = {
    var prefix = url.split("/", 2)(1)
    if (!prefix.endsWith("/")) {
      prefix += "/"
    }
    val bucketName = url.split(":").last.takeWhile(_ != '/')
    new S3RemoteClient(url, bucketName, prefix)
  }
}

class S3RemoteClient private(val url: String, val bucketName: String, val prefix: String) extends RemoteClient with Utils {
  lazy val client = AmazonS3ClientBuilder.defaultClient()

  override def get(path: BackupPath, file: File): Try[Unit] = ???

  override def list(): Try[Seq[RemoteFile]] = {
    Try {
      val listing = client.listObjects(bucketName, prefix)
      listing.getObjectSummaries.asScala.toSeq.map { summary =>
        val path = summary.getKey.replace(prefix, "")
        val size = summary.getSize
        new S3RemoteFile(BackupPath(path), size, summary.getETag)
      }
    }
  }

  def putMultipartThrottled(file: File, context: Option[RemoteOperationContext], md5Hash: Option[Array[Byte]], remotePath: String): Try[Unit] = {
    val metadata = createMetadata(md5Hash, Some(file.length()))
    Try {
      new MultipartUploader(bucketName, file, remotePath, context, metadata)
    }
  }

  override def put(file: File, path: BackupPath, context: Option[RemoteOperationContext], md5Hash: Option[Array[Byte]] = None): Try[Unit] = {
    val bufferSize = RequestClientOptions.DEFAULT_STREAM_BUFFER_SIZE
    val remotePath: String = computeRemotePath(path)
    md5Hash.foreach(hash => logger.info(s"File $file uploading with hash ${Hash(hash).base64}"))
    // slow
        slowUpload(file, context, md5Hash, bufferSize, remotePath)
//    putMultipartThrottled(file, context, md5Hash, remotePath)
    // fast, but no throttling
//            putFast(file, context, md5Hash, remotePath)
  }

  private def slowUpload(file: File, context: Option[RemoteOperationContext], md5Hash: Option[Array[Byte]], bufferSize: Int, remotePath: String): Try[Unit] = {
    tryWithResource(new ThrottlingInputStream(new BufferedInputStream(new FileInputStream(file), 2 * bufferSize), context)) { stream =>
      val manager = TransferManagerBuilder.standard().build()
      val metadata = createMetadata(md5Hash, Some(file.length()))
      val upload = manager.upload(bucketName, remotePath, stream, metadata)
      upload.waitForUploadResult()
      ()
    }
  }

  def createMetadata(md5Hash: Option[Array[Byte]], size: Option[Long] = None): ObjectMetadata = {
    val metadata = new ObjectMetadata()
    md5Hash.foreach { content =>
      val str = Utils.encodeBase64(content)
      metadata.setContentMD5(str)
    }
    size.foreach(metadata.setContentLength)
    metadata
  }

  private def putFast(file: File, context: Option[RemoteOperationContext], md5Hash: Option[Array[Byte]], remotePath: String) = {
    Try {
      val manager = TransferManagerBuilder.defaultTransferManager()
      val request = new PutObjectRequest(bucketName, remotePath, file)
      request.setMetadata(createMetadata(md5Hash))
      val upload = manager.upload(request)
      context.foreach { c =>
        c.initCounter(file.length(), file.getName)
        upload.addProgressListener(new ProgressListener {
          override def progressChanged(progressEvent: ProgressEvent): Unit = {
            val transferred = progressEvent.getBytesTransferred
            c.progress += transferred
          }
        })
      }
      upload.waitForUploadResult()
      ()
    }
  }

  private def computeRemotePath(path: BackupPath) = {
    val remotePath = prefix + path.path
    remotePath
  }

  override def exists(path: BackupPath): Try[Boolean] = ???

  override def delete(path: BackupPath): Try[Unit] = {
    Try {
      client.deleteObject(bucketName, computeRemotePath(path))
    }
  }

  override def getSize(path: BackupPath): Try[Long] = {
    Try {
      ???
    }
  }
}

import java.util

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{AbortMultipartUploadRequest, CompleteMultipartUploadRequest, InitiateMultipartUploadRequest, InitiateMultipartUploadResult, PartETag, UploadPartRequest}

class MultipartUploader(existingBucketName: String, file: File, remotePath: String, context: Option[RemoteOperationContext], metadata: ObjectMetadata) extends Utils {
  val s3Client: AmazonS3 = TransferManagerBuilder.standard().build().getAmazonS3Client()

  // Create a list of UploadPartResponse objects. You get one of these for
  // each part upload.
  val partETags: util.List[PartETag] = new util.ArrayList[PartETag]

  // Step 1: Initialize.
  val initRequest: InitiateMultipartUploadRequest = new InitiateMultipartUploadRequest(existingBucketName, remotePath)
  initRequest.setObjectMetadata(metadata)

  val initResponse: InitiateMultipartUploadResult = s3Client.initiateMultipartUpload(initRequest)

  val contentLength: Long = file.length
  var partSize: Long = 20 * 1024 * 1024 // Set part size to 5 MB.

  try {
    // Step 2: Upload parts.
    var filePosition: Long = 0
    var i: Int = 1
    while ( {
      filePosition < contentLength
    }) {
      // Last part can be less than 5 MB. Adjust part size.
      partSize = Math.min(partSize, (contentLength - filePosition))
      val stream = new ThrottlingInputStream(new PartialInputStream(file, filePosition, partSize), context)
      val request: UploadPartRequest = new UploadPartRequest()
        .withBucketName(existingBucketName)
        .withKey(remotePath)
        .withUploadId(initResponse.getUploadId)
        .withPartNumber(i)
        .withPartSize(partSize)
        .withInputStream(stream)
      request.setGeneralProgressListener((progressEvent: ProgressEvent) => {
        if (progressEvent.getEventType == ProgressEventType.TRANSFER_PART_COMPLETED_EVENT) {
          logger.info(s"Closing stream for part ${request.getPartNumber}")
          request.getInputStream.close()
        }
      })
      // Create request to upload a part.
      // Upload part and add response to our list.
      val result = s3Client.uploadPart(request)
      partETags.add(result.getPartETag)
      filePosition += partSize
      i += 1
    }
    // Step 3: Complete.
    val compRequest: CompleteMultipartUploadRequest = new CompleteMultipartUploadRequest(existingBucketName, remotePath, initResponse.getUploadId, partETags)
    s3Client.completeMultipartUpload(compRequest)
  } catch {
    case e: Exception =>
      s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(existingBucketName, remotePath, initResponse.getUploadId))
  }
}

class PartialInputStream(file: File, offset: Long, length: Long) extends InputStream {
  require(offset + length <= file.length())
  lazy val (in, bounded) = {
    val fis = new FileInputStream(file)
    fis.skip(offset)
    val bounded = new BoundedInputStream(fis, length)
    (fis, bounded)
  }

  override def read(): Int = {
    bounded.read()
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    bounded.read(b, off, len)
  }

  override def close(): Unit = {
    bounded.close()
    in.close()
    super.close()
  }
}