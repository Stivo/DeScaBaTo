package backup

import org.scalatest._
import java.io.File
import org.scalatest.Matchers._
import java.util.Arrays
import scala.collection.mutable.HashMap
import scala.collection.mutable.Buffer
import scala.collection.mutable.Set
import java.io.ByteArrayOutputStream
import scala.collection.mutable.ArrayBuffer
import java.util.{List => JList}
import java.util.ArrayList
import scala.collection.convert.DecorateAsScala
import scala.collection.JavaConversions._
import java.util.Properties
import java.io.FileInputStream
import java.io.FileInputStream

class RemoteClientSpecOld extends FlatSpec with BeforeAndAfterAll with BeforeAndAfter with PrepareBackupTestData with FileDeleter {

  ConsoleManager.testSetup
  var port = 0
  override def beforeAll {
    prepare
    port = FtpServer.server
  }
 
  implicit val folder = new BackupOptions()
  
  def fixture =
    new {
    
    }

  "local files " should "work as client" in {
    val temp = new File(testdata, "testLocal")
    writtenFolders += temp
    val backend = new VfsBackendClient(temp.toURI().toASCIIString())
    backend.put(new File(backupFrom, readme))
    backend.put(new File(backupFrom, "empty.txt"))
    (backend.list should contain allOf ("empty.txt", readme))
    backend.delete(readme)
    (backend.list should contain only ("empty.txt"))
    (backend.exists(readme) should not be true)
    backend.put(new File(readme))
    (backend.exists(readme) should be)
    val localCopy = new File(testdata, readme)
    backend.get(localCopy)
    (localCopy.exists() should be)
    (localCopy.length() should be > (0L))
  }
  
  "ftp " should "work as client" in {
	val ftpUrl = s"ftp://testvfs:asdfasdf@localhost:$port/"
	val backend = new VfsBackendClient(ftpUrl)
	assume(backend.list.isEmpty, "please provide an empty folder")
	backend.put(new File(backupFrom, readme))
	backend.put(new File(backupFrom, "empty.txt"))
	(backend.list should contain allOf ("empty.txt", readme))
	backend.delete(readme)
	(backend.list should contain only ("empty.txt"))
	(backend.exists(readme) should not be true)
	backend.put(new File(readme))
	(backend.exists(readme) should be)
	val localCopy = new File(testdata, readme)
	backend.get(localCopy)
	(localCopy.exists() should be)
	(localCopy.length() should be > (0L))
	(backend.delete(readme))
	(backend.delete("empty.txt"))
	(backend.list shouldBe 'empty)
	backend.close()
  }
  
  after {
    deleteFiles
  }
  
  override def afterAll {
    Actors.stop
    FtpServer.stop(port)
    deleteAll(new File("testftp"+port))
  }
  
}
