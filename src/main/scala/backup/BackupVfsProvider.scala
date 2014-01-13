package backup

import java.util.Collections
import org.apache.commons.vfs2.Capability
import org.apache.commons.vfs2.FileSystemOptions
import org.apache.commons.vfs2.FileName
import java.util.Collection
import java.util.ArrayList
import org.apache.commons.vfs2.provider.FileProvider
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider
import org.apache.commons.vfs2.provider.AbstractFileSystem
import org.apache.commons.vfs2.provider.AbstractFileName
import org.apache.commons.vfs2.FileObject
import org.apache.commons.vfs2.provider.AbstractLayeredFileProvider
import org.apache.commons.vfs2.provider.AbstractFileObject
import scala.collection.mutable.Buffer
import org.apache.commons.vfs2.{FileType => VfsFileType}
import java.nio.file.FileSystemException
import org.apache.commons.vfs2.FileSystem
import java.io.ByteArrayInputStream
import org.apache.commons.vfs2.provider.LayeredFileName
import org.apache.commons.vfs2.provider.local.LocalFileName
import java.io.File
import org.apache.commons.vfs2.impl.StandardFileSystemManager
import org.apache.commons.vfs2.provider.jar.JarFileSystem
import org.apache.commons.vfs2.provider.zip.ZipFileSystem
import org.apache.commons.vfs2.provider.jar.JarFileProvider
import java.util.Arrays
import java.util.Date
import pl.otros.vfs.browser.demo.TestBrowser

object BackupVfsProvider {
  
  import Capability._
  val capabilities = List(
            GET_TYPE,
            LIST_CHILDREN,
            READ_CONTENT,
            LAST_MODIFIED,
            GET_LAST_MODIFIED,
            READ_CONTENT,
            COMPRESS,
            VIRTUAL);
  
  // TODO this is a hack
  var passphrase : String = null
  
}

class BackupVfsProvider extends AbstractLayeredFileProvider with FileProvider {

    def doCreateFileSystem(scheme: String, file: FileObject, fileSystemOptions: FileSystemOptions) = {
        val rootName =
            new LayeredFileName(scheme, file.getName(), FileName.ROOT_PATH, VfsFileType.FOLDER);
        new BackupFileSystem(rootName, file, fileSystemOptions);
    }

    def getCapabilities : Collection[Capability] = {
    	val list = new ArrayList[Capability]()
    	BackupVfsProvider.capabilities.foreach{ e =>
    		list.add(e)
    	}
    	list
    }
 
}

class BackupFileSystem(name: AbstractFileName, fo: FileObject, options: FileSystemOptions) extends AbstractFileSystem(
    name, fo, options) with Serializable {
  
   protected def addCapabilities(caps: Collection[Capability]) {
     BackupVfsProvider.capabilities.foreach {
       c => caps.add(c)
     }
   }
   
   var indexes = Map[String, Index]()
   
   protected def createFile(name: AbstractFileName): FileObject = {
     val path = name.toString.drop(7).drop(6).takeWhile(_!='!')
      if (!( indexes contains path)) {
        val bof = new RestoreOptions() 
        bof.backupFolder = new File(path)
        val index = new Index(bof)
        bof.passphrase = BackupVfsProvider.passphrase
        index.loadIndex()
        indexes += ((path, index))
      }
      val index = indexes(path)
  	  new BackupFileObject(name, index, this);
    }
} 

case class FakeBackupFolder(val path: String) extends BackupPart {
  val attrs: FileAttributes = null
  val size = 0L
}

class BackupFileObject(name: AbstractFileName, index: Index, fs: AbstractFileSystem) extends AbstractFileObject(name, fs) with FileObject {
  
  val nameInBackup = name.toString.dropWhile(_!='!').drop(2)
  
  def pathParts(x: Any) : Seq[String] = x match {
    case x: String => x.split("[\\/]")
    case Some(x: BackupPart) => x.path.split("[\\/]")
    case x => println("Did not match " +x); Nil 
  }
  
  
  protected override def doGetLastModifiedTime() = {
    if (backupPart.attrs != null) {
      backupPart.attrs.get("lastModifiedTime").toString.toLong
    } else {
      new Date().getTime()
    }
  }
  
  def normalize(x: String) = x.replace('\\', '/')
  
  lazy val childrenOfPath = {
    val out = if (nameInBackup == "") {
      index.oldBackupFiles.map(_.path.take(3)).toSet.toSeq.sorted
   } else {
      val candidates = index.oldBackupFiles.filter(x => normalize(x.path).startsWith(nameInBackup))
      candidates.map(_.path.drop(nameInBackup.length()+1).takeWhile(x => x!='\\' && x!='/')).toSet.toSeq.sorted
   }
    out.filter(_!="")
  }
  
  lazy val backupPart : BackupPart = {
      index.oldBackupFiles.find(x => normalize(x.path) == nameInBackup) match {
        case Some(x) => x
        case None =>  {
          new FakeBackupFolder(nameInBackup)
        }
      }
    }
  
    /**
     * Determines if this file can be written to.
     *
     * @return <code>true</code> if this file is writeable, <code>false</code> if not.
     * @throws FileSystemException if an error occurs.
     */
    override def isWriteable() = {
        false;
    }

    /**
     * Returns the file's type.
     */
    def doGetType() = {
      backupPart match {
        case x: FolderDescription => VfsFileType.FOLDER
        case x: FileDescription => VfsFileType.FILE
        case x: FakeBackupFolder => VfsFileType.FOLDER
        case _ => VfsFileType.IMAGINARY
      }
    }

    /**
     * Lists the children of the file.
     */
    @Override
    def doListChildren() = {
      childrenOfPath.toArray
    }
    
    protected def doGetContentSize() = backupPart.size

    protected def doGetInputStream() = index.getInputStream(backupPart.asInstanceOf[FileDescription])
    
}

class Index(options: RestoreOptions) 
	extends RestoreHandler(options) {
  import Utils._
    
  def loadIndex() {
    loadBackupProperties()
    l.info("Loading information")
    oldBackupFiles.last
    val commonPart = oldBackupFiles.minBy(x => x.path.size)
    println(commonPart)
  }
}