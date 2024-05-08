package ch.descabato

import better.files.DisposeableExtensions
import ch.descabato.core.PasswordWrongException
import ch.descabato.core.actions.DoBackup
import ch.descabato.core.actions.DoRestore
import ch.descabato.core.config.BackupConfigurationHandler
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.config.BackupVerification
import ch.descabato.core.config.BackupVerification.BackupDoesntExist
import ch.descabato.core.config.BackupVerification.OK
import ch.descabato.core.config.BackupVerification.PasswordNeeded
import ch.descabato.core.model.BackupEnv
import ch.descabato.core.util.FileManager
import ch.descabato.frontend.BackupFolderOption
import ch.descabato.frontend.Command3
import ch.descabato.frontend.CommandRunner
import ch.descabato.frontend.CreateBackupOptions
import ch.descabato.frontend.HelpCommand
import ch.descabato.frontend.MultipleBackupConf
import ch.descabato.frontend.ProgramOption
import ch.descabato.frontend.ProgressReporters
import ch.descabato.frontend.RestoreConf
import ch.descabato.frontend.commands.CountCommand
import ch.descabato.frontend.commands.UploadCommand
import ch.descabato.remote.RemoteOptions
import ch.descabato.utils.Utils
import ch.descabato.utils.Utils.logException
import com.typesafe.scalalogging.Logger
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.slf4j.LoggerFactory

import java.io.File
import java.io.PrintStream
import java.nio.file.FileSystems
import java.security.Security
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

class RestoreCommand3(parsedConf: RestoreConf, backupFolderConf: BackupFolderConfiguration) extends Command3 {

  def run(): Unit = {
    for (backupEnv <- BackupEnv(backupFolderConf, readOnly = false).autoClosed) {
      parsedConf.printConfiguration()
      validateFilename(parsedConf.restoreToFolder)
      validateFilename(parsedConf.restoreInfo)
      for (restore <- new DoRestore(backupFolderConf).autoClosed) {
        if (parsedConf.restoreBackup.isSupplied) {
          restore.restoreByRevision(parsedConf, parsedConf.restoreBackup().toInt)
        } else {
          restore.restoreLatest(parsedConf)
        }
      }
    }

    def validateFilename(option: ScallopOption[String]): Unit = {
      if (option.isDefined) {
        val s = option()
        try {
          // validate the restore to folder, as this will throw an exception
          FileSystems.getDefault().getPath(s)
        } catch {
          case e: Exception =>
            System.err.println(s"$s for ${option.name} is not a valid filename: ${e.getMessage}")
            System.exit(1)
        }
      }
    }
  }

}

object Main {

  var paused: Boolean = false

  var lastErrors: Long = 0L

  def runsInJar: Boolean = classOf[CreateBackupOptions].getResource("CreateBackupOptions.class").toString.startsWith("jar:")

  def main(args: Array[String]): Unit = {
    try {
      Security.addProvider(new BouncyCastleProvider())
      java.lang.System.setOut(new PrintStream(System.out, true, "UTF-8"))
      new CommandRunner(args.toIndexedSeq).runCommand()
      exit(0)
    } catch {
      case e@PasswordWrongException(m, cause) =>
        val l: Logger = Logger(LoggerFactory.getLogger(getClass.getName))
        l.warn(m)
        logException(e)
        exit(1)
      case e@BackupVerification.BackupDoesntExist =>
        val l: Logger = Logger(LoggerFactory.getLogger(getClass.getName))
        l.warn(e.getMessage)
        logException(e)
        exit(2)
      case e: Throwable =>
        val l: Logger = Logger(LoggerFactory.getLogger(getClass.getName))
        l.warn("Program stopped due to exception", e)
        exit(3)
    }
  }

  def exit(exitCode: Int): Unit = {
    System.exit(exitCode)
  }

}
