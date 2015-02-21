package ch.descabato.frontend

import java.io.{File, PrintStream}
import java.security.Security

import ch.descabato.core._
import ch.descabato.utils.{FileUtils, Utils}
import org.bouncycastle.jce.provider.BouncyCastleProvider

object CLI extends Utils {

  var paused: Boolean = false

  var lastErrors: Long = 0L

  def runsInJar = classOf[CreateBackupOptions].getResource("CreateBackupOptions.class").toString.startsWith("jar:")

  def getCommands() = List(
    new BackupCommand(),
    new VerifyCommand(),
    new RestoreCommand(),
    new ReflectionCommand("browse", "ch.descabato.browser.BrowseCommand"),
    new HelpCommand()).map(x => (x.name.toLowerCase, x)).toMap

  def getCommand(name: String): Command = getCommands.get(name.toLowerCase()) match {
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
    if (System.getProperty("logname") == null)
      System.setProperty("logname", "backup.log")
    try {
      Security.addProvider(new BouncyCastleProvider())
      if (runsInJar) {
        java.lang.System.setOut(new PrintStream(System.out, true, "UTF-8"))
        parseCommandLine(args)
        exit(0)
      } else {
        //Thread.sleep(10000)
        try {
//          FileUtils.deleteAll(new File("L:/testdata/restore_old1"))
//            FileUtils.deleteAll(new File("L:/asdf2"))
        } catch {
          case x: Exception =>
        }
//        parseCommandLine("backup --threads 1 --serializer-type json --hash-algorithm md5 --compression none --volume-size 10mb C:/Users/Stivo/workspace-luna/DeScaBaTo/integrationtest/testdata/backup1 C:/Users/Stivo/workspace-luna/DeScaBaTo/integrationtest/testdata/input".split(" "))
        //
        //        parseCommandLine("backup --threads 1 L:\\testdata\\backup1 L:\\testdata\\input1".split(" "))
        //        parseCommandLine("verify --percent-of-files-to-check 100 L:\\testdata\\backup1".split(" "))
                parseCommandLine("restore --restore-to-folder L:\\testdata\\restore1 L:\\testdata\\backup1".split(" "))

        //        parseCommandLine("backup --threads 1 --compression gzip L:/asdf2 l:/testdata".split(" "))
        //        parseCommandLine("restore --no-ansi --no-gui --restore-backup backup_2015-02-21.070952.095_0.kvs --restore-to-folder L:\\testdata\\restore_old1 L:\\testdata\\backup_old1".split(" "))
//          parseCommandLine("restore --restore-to-folder L:/asdf2 D:/temp/backupasdf".split(" "))

//        parseCommandLine("backup --threads 40 --compression gzip D:/testdata/backup1 L:/testdata/input1".split(" "))
//        parseCommandLine("restore --restore-to-folder L:/testdata/restore1 L:/testdata/backup1".split(" "))

        //parseCommandLine("restore --restore-to-folder C:/Users/Stivo/workspace-luna/DeScaBaTo/integrationtest/testdata/restore1 C:/Users/Stivo/workspace-luna/DeScaBaTo/integrationtest/testdata/backup1".split(" "))

//         parseCommandLine("backup --serializer-type json --compression gzip --volume-size 50mb l:/testdata/backup1 l:/testdata/input1".split(" "))

        // parseCommandLine("backup --serializer-type json --volume-size 5mb backups ..\\testdata".split(" "))
        //parseCommandLine("backup --serializer-type json --hash-algorithm sha-256 --compression gzip --volume-size 100mb e:/temp/desca9 d:/pics/tosort".split(" "))

//        parseCommandLine("backup --passphrase testasdf --threads 10 --serializer-type json --compression lz4 --volume-size 100mb L:/desca8 L:/tmp".split(" "))

//        parseCommandLine("verify --passphrase testasdf --percent-of-files-to-check 100 L:\\desca8".split(" "))
//        parseCommandLine("restore --restore-to-folder F:/restore f:/desca8".split(" "))
        // parseCommandLine("backup --no-redundancy --serializer-type json --compression none --volume-size 5mb backups /home/stivo/progs/eclipse-fresh".split(" "))
        //        parseCommandLine("verify e:\\backups\\pics".split(" "))
        //              parseCommandLine("restore --help".split(" "))
//                      parseCommandLine("browse -p testasdf L:/desca8".split(" "))
        //        parseCommandLine("restore --passphrase mypasshere --restore-to-folder restore --relative-to-folder . backups".split(" "))
        exit(0)
      }
    } catch {
      case e@PasswordWrongException(m, cause) =>
        l.warn(m)
        logException(e)
        exit(-1)
      case e@BackupVerification.BackupDoesntExist =>
        l.warn(e.getMessage)
        logException(e)
        exit(-2)
      case e: Error =>
        l.warn(e.getMessage)
        logException(e)
        exit(-3)
    }
  }

  def exit(exitCode: Int) {
    System.exit(exitCode)
  }

}

