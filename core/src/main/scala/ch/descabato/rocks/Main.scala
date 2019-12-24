package ch.descabato.rocks

import java.io.PrintStream
import java.security.Security

import ch.descabato.core.PasswordWrongException
import ch.descabato.core.config.BackupVerification
import ch.descabato.frontend.Command
import ch.descabato.frontend.CreateBackupOptions
import ch.descabato.frontend.HelpCommand
import ch.descabato.utils.Utils
import org.bouncycastle.jce.provider.BouncyCastleProvider

object Main extends Utils {

  var paused: Boolean = false

  var lastErrors: Long = 0L

  def runsInJar: Boolean = classOf[CreateBackupOptions].getResource("CreateBackupOptions.class").toString.startsWith("jar:")

  def getCommands(): Map[String, Command] = List(
    new BackupCommand(),
    //    new VerifyCommand(),
    new RestoreCommand(),
    //    new ReflectionCommand("browse", "ch.descabato.ui.BrowseCommand"),
    //    new HelpCommand(),
    //    new VersionCommand()
  ).map(x => (x.name, x)).toMap

  def getCommand(name: String): Command = getCommands().get(name.toLowerCase()) match {
    case Some(x) => x
    case None => l.warn("No command named " + name + " exists."); new HelpCommand()
  }

  def parseCommandLine(args: Seq[String]) {
    val (command, tail) = if (args.isEmpty) {
      ("help", Nil)
    } else {
      (args.head, args.tail)
    }
    getCommand(command).execute(tail)
  }

  def main(args: Array[String]) {
    if (System.getProperty("logname") == null) {
      System.setProperty("logname", "backup.log")
    }
    try {
      Security.addProvider(new BouncyCastleProvider())
      java.lang.System.setOut(new PrintStream(System.out, true, "UTF-8"))
      parseCommandLine(args)
      exit(0)
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

  def exit(exitCode: Int) {
    System.exit(exitCode)
  }

}
