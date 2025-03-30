package ch.descabato.frontend

import ch.descabato.core.config.BackupConfigurationHandler
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.config.BackupVerification
import ch.descabato.core.config.BackupVerification.BackupDoesntExist
import ch.descabato.core.config.BackupVerification.OK
import ch.descabato.core.config.BackupVerification.PasswordNeeded
import ch.descabato.core.util.FileManager
import ch.descabato.utils.Utils
import com.typesafe.scalalogging.Logger
import org.rogach.scallop.ScallopConf
import org.slf4j.LoggerFactory

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

abstract class Command extends Utils {
  def run(): Unit
}

trait BackupConfCommandCreator extends BackupFolderOption {
  def runCommand(backupFolderConf: BackupFolderConfiguration): Unit

  def needsExistingBackup: Boolean
}

trait SimpleCommandCreator extends ScallopConf {
  def runCommand(): Unit
}

class CommandRunner(args: Seq[String]) {
  private val (commandName, tailArgs) = if (args.isEmpty) {
    ("help", Nil)
  } else {
    (args.head, args.tail)
  }

  private val commandsWithFolder: Map[String, Seq[String] => BackupConfCommandCreator] = Map(
    "backup" -> { (args: Seq[String]) => new MultipleBackupConf(args) },
    "restore" -> { (args: Seq[String]) => new RestoreConf(args) },
    "verify" -> { (args: Seq[String]) => new VerifyConf(args) },
    "upload" -> { (args: Seq[String]) => new UploadConf(args) },
    "checkfiles" -> { (args: Seq[String]) => new CheckFilesConf(args) },
  ) ++ {
    scala.util.Try {
      Class.forName("ch.descabato.rocks.fuse.FuseMountConf")
    }.map { clas =>
      Map("mount" -> { (args: Seq[String]) =>
        val constructor = clas.getConstructor(classOf[Seq[String]])
        constructor.newInstance(args).asInstanceOf[BackupConfCommandCreator]
      })
    }.getOrElse(Map()) ++ {
      scala.util.Try {
        Class.forName("ch.descabato.web.WebServeConf")
      }.map { clas =>
        Map("serve" -> { (args: Seq[String]) =>
          val constructor = clas.getConstructor(classOf[Seq[String]])
          constructor.newInstance(args).asInstanceOf[BackupConfCommandCreator]
        })
      }.getOrElse(Map())
    }
  }

  private val commandsWithoutFolder: Map[String, Seq[String] => SimpleCommandCreator] = Map(
    "count" -> { (args: Seq[String]) => new CountConf(args) },
    "help" -> { (args: Seq[String]) => new GenericConf(args, new HelpCommand(this)) },
    "--version" -> { (args: Seq[String]) => new GenericConf(args, new VersionCommand()) },
  )

  def allCommands(): List[String] = (commandsWithFolder.keys ++ commandsWithoutFolder.keys)
    .filterNot(_.startsWith("--"))
    .toList
    .sorted

  def runCommand(): Unit = {
    if (commandsWithFolder.contains(commandName)) {
      startCommandWithFolder()
    } else if (commandsWithoutFolder.contains(commandName)) {
      startCommandWithoutFolder()
    } else {
      new HelpCommand(this).execute(tailArgs)
    }
  }

  private def askUser(question: String = "Do you want to continue?", mask: Boolean = false): String = {
    println(question)
    if (mask)
      System.console().readPassword().mkString
    else
      System.console().readLine()
  }

  private def askUserYesNo(question: String = "Do you want to continue?"): Boolean = {
    val answer = askUser(question)
    val yes = Set("yes", "y")
    if (yes.contains(answer.toLowerCase().trim)) {
      true
    } else {
      println("User aborted")
      false
    }
  }

  private def startCommandWithFolder(): Unit = {

    val parsedArgs = commandsWithFolder.getOrElse(commandName,
        throw new IllegalArgumentException(s"Commmand $commandName doesn't exist"))
      .apply(tailArgs)
    parsedArgs.verify()
    // this cast is fine because in this Map we only have BackupFolderOption configs
    val backupFolderOption = parsedArgs.asInstanceOf[BackupFolderOption]
    val destination = backupFolderOption.backupDestination.getOrElse(
      throw new IllegalArgumentException("Must set backup destination")
    )
    val logfile = backupFolderOption.logfile.getOrElse {
      val now = LocalDateTime.now()
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HHmmss")
      val date = now.format(formatter)
      s"$commandName-$date.log"
    }

    parsedArgs match {
      case ng: NoGuiOption =>
        if (ng.noGui.isSupplied && ng.noGui()) {
          ProgressReporters.guiEnabled = false
        }
      case _ => // pass
    }

    System.setProperty("logname", new File(destination, "logs/" + logfile).getAbsolutePath)
    // now the loggers are ready
    val version = System.getProperty("prog.version")
    val revision = System.getProperty("prog.revision")
    val logger = Logger(LoggerFactory.getLogger(getClass.getName))
    logger.info(s"Descabato version ${version} (revision ${revision})")

    backupFolderOption.printConfiguration()

    val confHandler = new BackupConfigurationHandler(backupFolderOption, parsedArgs.needsExistingBackup)
    confHandler.verify() match {
      case b@BackupDoesntExist => throw BackupDoesntExist
      case PasswordNeeded =>
        val passphrase = askUser("This backup is passphrase protected. Please type your passphrase.", mask = true)
        confHandler.setPassphrase(passphrase)
      case OK =>
    }
    val conf = confHandler.updateAndGetConfiguration()
    val manager = new FileManager(conf)
    // TODO
    //    if (checkForUpgradeNeeded && (manager.metadata.getFiles().nonEmpty || manager.volumeIndex.getFiles().nonEmpty || manager.backup.getFiles().nonEmpty)) {
    //      throw ExceptionFactory.createUpgradeException(conf.version)
    //    }
    parsedArgs.runCommand(conf)
  }

  private def startCommandWithoutFolder(): Unit = {
    val conf = commandsWithoutFolder.getOrElse(commandName,
      throw new IllegalArgumentException(s"Command $commandName doesn't exist"))(tailArgs)
    conf.verify()
    // these command should not log
    conf.runCommand()
  }

}