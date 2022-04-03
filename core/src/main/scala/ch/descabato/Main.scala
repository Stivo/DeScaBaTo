package ch.descabato

import ch.descabato.core.PasswordWrongException
import ch.descabato.core.config.BackupVerification
import ch.descabato.frontend.Command
import ch.descabato.frontend.CreateBackupOptions
import ch.descabato.frontend.HelpCommand
import ch.descabato.frontend.ReflectionCommand
import ch.descabato.frontend.commands.BackupCommand
import ch.descabato.frontend.commands.CountCommand
import ch.descabato.frontend.commands.RestoreCommand
import ch.descabato.frontend.commands.UploadCommand
import ch.descabato.frontend.commands.VerifyCommand
import ch.descabato.utils.Utils
import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.io.PrintStream
import java.security.Security
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Main extends Utils {

  var paused: Boolean = false

  var lastErrors: Long = 0L

  def runsInJar: Boolean = classOf[CreateBackupOptions].getResource("CreateBackupOptions.class").toString.startsWith("jar:")

  def getCommands(): Map[String, Command] = List(
    new BackupCommand(),
    new VerifyCommand(),
    new UploadCommand(),
    new CountCommand(),
    new RestoreCommand(),
    new ReflectionCommand("mount", "ch.descabato.rocks.fuse.MountCommand"),
    //    new HelpCommand(),
    //    new VersionCommand()
  ).map(x => (x.name, x)).toMap

  def getCommand(name: String): Command = getCommands().get(name.toLowerCase()) match {
    case Some(x) => x
    case None => l.warn("No command named " + name + " exists."); new HelpCommand()
  }

  def parseCommandLine(args: Seq[String]): Unit = {
    val (command, tail) = if (args.isEmpty) {
      ("help", Nil)
    } else {
      (args.head, args.tail)
    }
    getCommand(command).execute(tail)
  }

  def main(args: Array[String]): Unit = {
    if (System.getProperty("logname") == null) {
      val date = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)
      System.setProperty("logname", s"backup-$date.log")
    }
    try {
      Security.addProvider(new BouncyCastleProvider())
      java.lang.System.setOut(new PrintStream(System.out, true, "UTF-8"))
      parseCommandLine(args.toIndexedSeq)
      //      exit(0)
    } catch {
      case e@PasswordWrongException(m, cause) =>
        l.warn(m)
        logException(e)
        exit(1)
      case e@BackupVerification.BackupDoesntExist =>
        l.warn(e.getMessage)
        logException(e)
        exit(2)
      case e: Throwable =>
        l.warn("Program stopped due to exception", e)
        exit(3)
    }
  }

  def exit(exitCode: Int): Unit = {
    System.exit(exitCode)
  }

}