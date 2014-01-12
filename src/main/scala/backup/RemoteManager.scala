package backup

import java.io.File
import org.apache.commons.vfs2.impl.StandardFileSystemManager
import org.apache.commons.vfs2.AllFileSelector
import com.sun.jna.platform.FileUtils
import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer
import akka.actor.Actor
import com.typesafe.config.Config
import akka.dispatch.BoundedMailbox
import scala.concurrent.duration.FiniteDuration
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.{ask, gracefulStop}
import scala.concurrent.Await
import scala.concurrent.duration._
import java.io.IOException
import java.io.InputStream
import backup.Streams.ReportingOutputStream

object Actors {
   var system = ActorSystem("HelloSystem")
  
   var remoteManager = system.actorOf(Props[FileUploadActor].withDispatcher("consumer-dispatcher"), "consumer")
   
   var testMode = false
   
   def stop() {
     if (testMode) {
       val fut = (remoteManager ? Configure(null)) (10 hours)
       Await.result(fut, 10 hours)
       system.shutdown
       system.awaitTermination
       system = ActorSystem("HelloSystem2")
       remoteManager = system.actorOf(Props[FileUploadActor].withDispatcher("consumer-dispatcher"), "consumer")
     } else {
	   val stopped = gracefulStop(remoteManager, 10 hours)
	   Await.result(stopped, 10 hours)
     }
   }

   def downloadFile(f: File) = {
     val wait = (remoteManager ? DownloadFile(f))(10 hours)
     Await.ready(wait, Duration.Inf)
     if (!f.exists) {
       throw new IOException("Did not successfully download "+f)
     }
   }
   
}

case class DownloadFile(file: File)
case class UploadFile(file: File, deleteLocalOnSuccess : Boolean = false)
case class Configure(options: BackupFolderOption)
case object DownloadMetadata 

class FileUploadActor extends Actor with Utils {
  
  var options: BackupFolderOption = null
  var backend : BackendClient = null
  
  private def localFile(s: String) = new File(options.backupFolder, s) 
  
  def reply { sender ! true }
  
  def receive = {
    case Configure(o) => {
      if (o == null) {
        options = null
      } else if (o.remote.url.isDefined) {
        options = o; backend = new VfsBackendClient(o.remote.url.get); 
      }
      reply
    }
    case _ if options == null || !options.remote.enabled => sender ! true // Ignore
    case DownloadFile(f) => {
      l.info("Downloading file "+f)
      backend.get(f)
      sender ! true
    }
    case DownloadMetadata => {
      // TODO use FileManager to correctly identify data?
      val files = backend.list.filter(!_.startsWith("volume_"))
      files.foreach { f=>
        if (localFile(f).length() < backend.getSize(f)) {
          l.info("Downloading file "+f)
          backend.get(localFile(f))
        }
      }
      l.info("Finished synchronizing")
      sender ! true
    }
    case UploadFile(file, true) => {
      l.info("Uploading file "+file)
      uploadAndDeleteLocal(file)
    }
    case UploadFile(file, false) => {
      l.info("Uploading file "+file)
      if (!isDone(file)) 
    	 backend.put(file)
      if (isDone(file))
         l.info(s"Successfully uploaded ${file.getName}")
    }
    case x => sender ! akka.actor.Status.Failure(new IllegalArgumentException("Received unknown message type"))
  }
    
  def isDone(f: File) = backend.exists(f.getName()) && backend.getSize(f.getName()) == f.length()
  
  def deleteLocal(f: File) = {
    FileUtils.getInstance().moveToTrash(Array(f));
  }
  
  def uploadAndDeleteLocal(f: File) {
    if (isDone(f)) {
      deleteLocal(f)
    } else {
      backend.put(f)
      if (isDone(f)) {
        l.info(s"Successfully uploaded ${f.getName}, deleting local copy")
        deleteLocal(f)
      } else {
    	l.info(s"Did not upload ${f.getName} correctly, will try again")
      }
    }
  }
  
  override def postStop {
    context.system.shutdown()
  }
  
}

trait BackendClient {
  def get(f: File, name: Option[String] = None)
  def put(f: File, name: Option[String] = None)
  def exists(name: String) : Boolean
  def delete(name: String) 
  def list() : Iterable[String]
  def getSize(name: String) : Long
}

class VfsBackendClient(url: String) extends BackendClient {
  val manager = new StandardFileSystemManager();
  manager.init()
  val remoteDir = manager.resolveFile(url); // TODO options?
  import org.apache.commons.vfs2.FileType
  def list() : Iterable[String] = {
    remoteDir.getChildren().filter(_.getType()==FileType.FILE).map(x => x.getName().getBaseName())
  }
  
  def delete(name: String) {
    remoteDir.resolveFile(name).delete()
  }

  def exists(name: String) = {
    remoteDir.resolveFile(name).exists()
  }
  
  def put(f: File, name: Option[String] = None) {
    val remoteName = name match {
      case Some(x) => x
      case None => f.getName
    }
    val from = manager.toFileObject(f)
    val to = remoteDir.resolveFile(remoteName)
    val in = from.getContent().getInputStream()
    val out =  to.getContent().getOutputStream()
    val reporter = new ReportingOutputStream(out, s"Uploading $remoteName", size = from.getContent().getSize())
    Streams.copy(in, reporter)
  }
  
  def get(f: File, name: Option[String] = None) {
    val remoteName = name match {
      case Some(x) => x
      case None => f.getName
    }
    val from = manager.toFileObject(f)
    from.copyFrom(remoteDir.resolveFile(remoteName), new AllFileSelector())
  }

  def getSize(name: String) = {
    if (!exists(name)) -1 else remoteDir.resolveFile(name).getContent().getSize()
  }
}