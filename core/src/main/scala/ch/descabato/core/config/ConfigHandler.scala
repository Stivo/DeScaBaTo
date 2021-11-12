package ch.descabato.core.config

import better.files._
import ch.descabato.core.BackupException
import ch.descabato.core.config.BackupVerification.OK
import ch.descabato.frontend.BackupFolderOption
import ch.descabato.frontend.ChangeableBackupOptions
import ch.descabato.frontend.CreateBackupOptions
import ch.descabato.utils.JsonSerialization
import ch.descabato.utils.Utils

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream

object InitBackupFolderConfiguration extends Utils {
  def apply(option: BackupFolderOption, passphrase: Option[String]): BackupFolderConfiguration = {
    val out = BackupFolderConfiguration(option.backupDestination(), passphrase)
    option match {
      case o: ChangeableBackupOptions =>
        o.keylength.foreach(out.keyLength = _)
        o.volumeSize.foreach(out.volumeSize = _)
        o.ignoreFile.foreach(f => out.ignoreFile = Some(f))
        //        o.renameDetection.foreach(out.renameDetection = _)
        //        o.noRedundancy.foreach(b => out.redundancyEnabled = !b)
        //        o.volumeRedundancy.foreach(out.volumeRedundancy = _)
        //        o.metadataRedundancy.foreach(out.metadataRedundancy = _)
        o.dontSaveSymlinks.foreach(b => out.saveSymlinks = !b)
        o.compression.foreach(x => out.compressor = x)
      case _ =>
    }
    option match {
      case o: CreateBackupOptions =>
        o.hashAlgorithm.foreach(out.hashAlgorithm = _)
        o.remoteUri.foreach(out.remoteOptions.uri = _)
        o.remoteMode.foreach(out.remoteOptions.mode = _)
      case _ => // TODO
    }
    out
  }

  def merge(old: BackupFolderConfiguration, supplied: BackupFolderOption, passphrase: Option[String]): (BackupFolderConfiguration, Boolean) = {
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
        if (o.ignoreFile.isSupplied) {
          o.ignoreFile.foreach(f => old.ignoreFile = Some(f))
          changed = true
        }
        //        if (o.renameDetection.isSupplied) {
        //          o.renameDetection.foreach(old.renameDetection = _)
        //          changed = true
        //        }
        //        if (o.noRedundancy.isSupplied) {
        //          o.noRedundancy.foreach(b => old.redundancyEnabled = !b)
        //          changed = true
        //        }
        //        if (o.volumeRedundancy.isSupplied) {
        //          o.volumeRedundancy.foreach(old.volumeRedundancy = _)
        //          changed = true
        //        }
        //        if (o.metadataRedundancy.isSupplied) {
        //          o.metadataRedundancy.foreach(old.metadataRedundancy = _)
        //          changed = true
        //        }
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
    old.verify()
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
class BackupConfigurationHandler(private var supplied: BackupFolderOption, existing: Boolean) extends Utils {

  var passphrase: Option[String] = supplied.passphrase.toOption

  val mainFile: String = "backup.json"
  val folder: File = supplied.backupDestination()

  private val configFile = new File(folder, mainFile)

  def hasOld: Boolean = configFile.exists() && loadOld().isDefined

  def loadOld(): Option[BackupFolderConfiguration] = {
    val json = new JsonSerialization()
    // TODO a backup.json that is invalid is a serious problem. Should throw exception
    json.readObject[BackupFolderConfiguration](new FileInputStream(configFile)) match {
      case Left(x) => Some(x)
      case Right(e) =>
        logger.warn(s"Could not read config file $configFile", e)
        None
    }
  }

  def verify(): BackupVerification.VerificationResult = {
    import BackupVerification._
    if (existing && !hasOld) {
      return BackupDoesntExist
    }
    if (hasOld) {
      if (loadOld().get.version < "0.6.0") {
        throw new IllegalArgumentException("Can not load old version of backup. Please use the appropriate version for it.")
      }
      if (loadOld().get.hasPassword && passphrase.isEmpty) {
        return PasswordNeeded
      }
    }
    OK
  }

  def setPassphrase(passphrase: String): Unit = {
    this.passphrase = Some(passphrase)
  }

  def verifyAndInitializeSetup(conf: BackupFolderConfiguration): Unit = {
    //    if (conf.redundancyEnabled && CommandLineToolSearcher.lookUpName("par2").isEmpty) {
    //      throw ExceptionFactory.newPar2Missing
    //    }
  }

  private def write(out: BackupFolderConfiguration): Unit = {
    for (fos <- new FileOutputStream(configFile).autoClosed) {
      fos.write(out.asJson())
    }
  }

  def updateAndGetConfiguration(): BackupFolderConfiguration = {
    assert(verify() == OK)
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
