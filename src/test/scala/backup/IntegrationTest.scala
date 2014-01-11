package backup

import org.scalatest.FlatSpec
import java.io.File
import org.scalatest.BeforeAndAfter
import scala.collection.mutable.Buffer
import org.scalatest._
import java.security.MessageDigest
import java.io.FileInputStream
import java.nio.file.Files

trait PrepareBackupTestData {
  lazy val testdata = new File("testdata")
  lazy val backupFrom = new File(testdata, "backupinput")
  def prepare {
    val folder = new File(backupFrom, "testFolder")
    if (!folder.exists())
      folder.mkdirs()
    val f = new File(backupFrom, "README.md")
    if (!f.exists) {
      Files.copy(new File("README.md").toPath(), f.toPath())
    }
    val f2 = new File(backupFrom, "empty.txt")
    if (!f2.exists) {
      f2.createNewFile()
    }
  }
}

trait FileDeleter {
  var writtenFolders = Buffer[File]()
  
  private def deleteAll(f: File) {
    def walk(f: File) {
      f.isDirectory() match {
        case true => f.listFiles().foreach(walk); f.delete()
        case false => f.delete()
      }
    }
    walk(f)
  }
  
  def deleteFiles {
    writtenFolders.foreach(deleteAll)
    writtenFolders.clear
  }

}

class IntegrationTest extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll with PrepareBackupTestData with FileDeleter {
  
  ConsoleManager.testSetup
  
  override def beforeAll {
    prepare
  }
  
  var bo = new BackupOptions()
  var ro = new RestoreOptions()
  
  before {
    bo = new BackupOptions()
    bo.backupFolder = new File(testdata, "temp/backup")
    bo.folderToBackup = List(backupFrom)
    ro = new RestoreOptions()
    ro.relativeToFolder = bo.folderToBackup.headOption
    ro.restoreToFolder = new File(testdata, "temp/restored")
    ro.backupFolder = bo.backupFolder
  }
  
  def getHandler(options: BackupOptions) = new BackupHandler(options)
  
  def getRestoreHandler(options: RestoreOptions) = new RestoreHandler(options)
  
  "backup" should "backup and restore" in {
    ro.restoreToFolder = new File(testdata, "temp/restored")
    ro.relativeToFolder = bo.folderToBackup.headOption
    testBackupAndRestore(bo, ro)
  }

  "backup with zip" should "backup and restore" in {
    bo.compression = CompressionMode.zip
    testBackupAndRestore(bo, ro)
  }

  "backup with encryption" should "backup and restore" in {
    bo.passphrase = "Test"
  	backup(bo)
    try{
    	restore(ro)
    	fail("Should not be able to restore ")
    } catch {
      case x: Exception =>  
    }
    ro.passphrase = "wrong key"
    try{
    	restore(ro)
    	fail("Should not be able to restore ")
    } catch {
      case x: Exception =>  
    }
    ro.passphrase = "Test"
    restore(ro)
    compareBackups(bo.folderToBackup.head, ro.restoreToFolder)
  }

  def backup(backupOptions: BackupOptions) {
    assert(false === backupOptions.backupFolder.exists())
    val handler = getHandler(backupOptions)
    writtenFolders += backupOptions.backupFolder
    handler.backupFolder
  }

  def restore(restoreOptions: RestoreOptions) {
    val handler = getRestoreHandler(restoreOptions)
    handler.restoreFolder
    writtenFolders += restoreOptions.restoreToFolder
  }
  
  def testBackupAndRestore(backupOptions: BackupOptions, restoreOptions: RestoreOptions) {
    backup(backupOptions)
    restore(restoreOptions)
    compareBackups(backupOptions.folderToBackup.head, restoreOptions.restoreToFolder)
  }
  
  def hash(f: File) = {
    val md = MessageDigest.getInstance("md5")
    val fh = new FileHandlingOptions() {}
    fh.compression = CompressionMode.none
    fh.passphrase = null
    val buf = Streams.readFully(new FileInputStream(f))(fh)
    Utils.encodeBase64(md.digest(buf))
  }
  
  def compareBackups(f1: File, f2: File) {
    val files1 = f1.listFiles().toSet
    val files2 = f2.listFiles().toSet
    assert(files1.map(_.getName) === files2.map(_.getName))
    var counter = 0
    for (c1 <- files1; c2 <- files2) {
      if (c1.getName.equalsIgnoreCase(c2.getName)) {
        counter += 1
        assert(c1.lastModified === c2.lastModified)
        assert(c1.getName === c2.getName)
        assert(c1.isDirectory() === c2.isDirectory())
        if (c1.isFile())
        	assert(hash(c1) === hash(c2))
      }
    }
    assert(files1.size === counter)
  }
  
  after {
    deleteFiles
  }
  
}