package backup

import java.io.File
import org.apache.commons.vfs2.impl.StandardFileSystemManager
import org.apache.commons.vfs2.FileType
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
import akka.pattern.gracefulStop
import scala.concurrent.Await
import scala.concurrent.duration._

object Actors {
   lazy val system = ActorSystem("HelloSystem")
  
   lazy val remoteManager = system.actorOf(Props[FileUploadActor].withDispatcher("consumer-dispatcher"), "consumer")
   
   def stop() {
	   val stopped = gracefulStop(remoteManager, 30 minutes)
	   Await.result(stopped, 30 minutes)
   }
}

case class UploadFile(file: File, deleteLocalOnSuccess : Boolean  = false)
case class Configure(options: BackupFolderOption)

class MyBoundedMailbox(settings: ActorSystem.Settings, config: Config) extends BoundedMailbox(5, FiniteDuration(30, "minutes"))

class FileUploadActor extends Actor with Utils with CountingFileManager {
  
  var options: BackupFolderOption = null
  var backendClient : BackendClient = null
  
  def receive = {
    case Configure(o) => options = o; backendClient = new VfsBackendClient(options.remote.url.get)
    case UploadFile(_, _) if !options.remote.enabled => // Ignore
    case UploadFile(file, true) => {
      l.info("Uploading file "+file)
      uploadAndDeleteLocal(file)
    }
    case UploadFile(file, false) => {
      l.info("Uploading file "+file)
      if (!isDone(file)) 
    	 backendClient.put(file)
      if (isDone(file))
         l.info(s"Successfully uploaded ${file.getName}")
    }
    case x => l.error("Received unknown message type") 
  }
  
  def uploadVolumes {
	val (files, num) = getFilesAndNextNum(options.backupFolder)("volume_")
	files.foreach { f=>
	  uploadAndDeleteLocal(f)
	}
  }
  
  def isDone(f: File) = backendClient.exists(f.getName()) && backendClient.getSize(f.getName()) == f.length()
  
  def delete(f: File) = {
    FileUtils.getInstance().moveToTrash(Array(f));
  }
  
  def uploadAndDeleteLocal(f: File) {
    if (isDone(f)) {
      delete(f)
    } else {
      backendClient.put(f)
      if (isDone(f)) {
        l.info(s"Successfully uploaded ${f.getName}, deleting local copy")
        delete(f)
      } else {
    	l.info(s"Did not upload ${f.getName} correctly, will try again")
      }
    }
  }
  
  override def postStop {
    println("Request shutdown")
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
    remoteDir.resolveFile(remoteName).copyFrom(from, new AllFileSelector())
  }
  
  def get(f: File, name: Option[String] = None) {
    val remoteName = name match {
      case Some(x) => x
      case None => f.getName
    }
    val from = manager.toFileObject(f)
    from.copyFrom(remoteDir.resolveFile(remoteName), new AllFileSelector())
  }

  def getSize(name: String) = remoteDir.resolveFile(name).getContent().getSize()
}