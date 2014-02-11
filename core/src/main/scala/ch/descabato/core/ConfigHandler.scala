package ch.descabato.core

import net.java.truevfs.access.TConfig
import ch.descabato.utils.JsonSerialization
import java.io.File
import ch.descabato.utils.Utils
import java.io.FileInputStream
import net.java.truevfs.kernel.spec.FsAccessOption
import ch.descabato.TrueVfs
import ch.descabato.frontend.ChangeableBackupOptions
import ch.descabato.frontend.BackupFolderOption
import ch.descabato.frontend.CreateBackupOptions
import java.io.FileOutputStream
import net.java.truevfs.kernel.spec.sl.FsDriverMapLocator


object InitBackupFolderConfiguration extends Utils {
  def apply(option: BackupFolderOption, passphrase: Option[String]) = {
    val out = BackupFolderConfiguration(option.backupDestination(), option.prefix(), passphrase)
    option match {
      case o: ChangeableBackupOptions =>
        o.keylength.foreach(out.keyLength = _)
        o.volumeSize.foreach(out.volumeSize = _)
        o.threads.foreach(out.threads = _)
        o.checkpointEvery.foreach(out.checkPointEvery = _)
        o.renameDetection.foreach(out.renameDetection = _)
        o.noRedundancy.foreach(b => out.redundancyEnabled = !b)
        o.volumeRedundancy.foreach(out.volumeRedundancy = _)
        o.metadataRedundancy.foreach(out.metadataRedundancy = _)
        o.dontSaveSymlinks.foreach(b => out.saveSymlinks = !b)
        o.compression.foreach(x => out.compressor = x)
      case _ =>
    }
    option match {
      case o: CreateBackupOptions =>
        o.serializerType.foreach(out.serializerType = _)
        o.blockSize.foreach(out.blockSize = _)
        o.hashAlgorithm.foreach(out.hashAlgorithm = _)
      case _ => // TODO
    }
    out
  }

  def merge(old: BackupFolderConfiguration, supplied: BackupFolderOption, passphrase: Option[String]) = {
    old.passphrase = passphrase
    var changed = false
    supplied match {
      case o: ChangeableBackupOptions =>
        if (o.keylength.isSupplied) {
          o.keylength.foreach(old.keyLength = _)
          changed = true
        }
        if (o.volumeSize.isSupplied) {
          o.volumeSize.foreach(old.volumeSize = _)
          changed = true
        }
        if (o.threads.isSupplied) {
          o.threads.foreach(old.threads = _)
          changed = true
        }
        if (o.checkpointEvery.isSupplied) {
          o.checkpointEvery.foreach(old.checkPointEvery = _)
          changed = true
        }
        if (o.renameDetection.isSupplied) {
          o.renameDetection.foreach(old.renameDetection = _)
          changed = true
        }
        if (o.noRedundancy.isSupplied) {
          o.noRedundancy.foreach(b => old.redundancyEnabled = !b)
          changed = true
        }
        if (o.volumeRedundancy.isSupplied) {
          o.volumeRedundancy.foreach(old.volumeRedundancy = _)
          changed = true
        }
        if (o.metadataRedundancy.isSupplied) {
          o.metadataRedundancy.foreach(old.metadataRedundancy = _)
          changed = true
        }
        if (o.dontSaveSymlinks.isSupplied) {
          o.dontSaveSymlinks.foreach(b => old.saveSymlinks = !b)
          changed = true
        }
        if (o.compression.isSupplied) {
          o.compression.foreach(x => old.compressor = x)
          changed = true
        }
      // TODO other properties that can be set again
      // TODO generate this code omg
      case _ =>
    }
    l.debug("Configuration after merge " + old)
    (old, changed)
  }

}

object BackupVerification {
  trait VerificationResult

  case object PasswordNeeded extends VerificationResult

  case object BackupDoesntExist extends Exception("This backup was not found.\nSpecify backup folder and prefix if needed")
    with VerificationResult with BackupException

  case object OK extends VerificationResult
}

/**
 * Loads a configuration and verifies the command line arguments
 */
class BackupConfigurationHandler(supplied: BackupFolderOption) extends Utils {

  val mainFile = supplied.prefix() + "backup.json"
  val folder: File = supplied.backupDestination()
  def hasOld = new File(folder, mainFile).exists() && loadOld().isDefined
  def loadOld(): Option[BackupFolderConfiguration] = {
    val json = new JsonSerialization()
    // TODO a backup.json that is invalid is a serious problem. Should throw exception
    json.readObject[BackupFolderConfiguration](new FileInputStream(new File(folder, mainFile))) match {
      case Left(x) => Some(x)
      case _ => None
    }
  }

  def write(out: BackupFolderConfiguration) {
    val json = new JsonSerialization()
    val fos = new FileOutputStream(new File(folder, mainFile))
    json.writeObject(out, fos)
    // writeObject closes
  }

  def verify(existing: Boolean): BackupVerification.VerificationResult = {
    import BackupVerification._
    if (existing && !hasOld) {
      return BackupDoesntExist
    }
    if (hasOld) {
      if (loadOld().get.hasPassword && supplied.passphrase.isEmpty) {
        return PasswordNeeded
      }
    }
    OK
  }

  def verifyAndInitializeSetup(conf: BackupFolderConfiguration) {
//    if (conf.redundancyEnabled && CommandLineToolSearcher.lookUpName("par2").isEmpty) {
//      throw ExceptionFactory.newPar2Missing
//    }
    def initTrueVfs(conf: BackupFolderConfiguration) {
      for (p <- conf.passphrase) {
        TConfig.current().setArchiveDetector(TrueVfs.newArchiveDetector1(FsDriverMapLocator.SINGLETON, ".zip.raes", p.toCharArray(), conf.keyLength))
      }
      TConfig.current().setAccessPreference(FsAccessOption.STORE, true)
      // Disabled to distinguish files being changed and completed files
      //TConfig.current().setAccessPreference(FsAccessOption.GROW, true)
    }
    initTrueVfs(conf)
  }

  def configure(passphrase: Option[String]): BackupFolderConfiguration = {
    if (hasOld) {
      val oldConfig = loadOld().get
      val (out, changed) = InitBackupFolderConfiguration.merge(oldConfig, supplied, passphrase)
      if (changed) {
        write(out)
      }
      verifyAndInitializeSetup(out)
      out
    } else {
      folder.mkdirs()
      val out = InitBackupFolderConfiguration(supplied, passphrase)
      write(out)
      verifyAndInitializeSetup(out)
      out
    }
  }

}