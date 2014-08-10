package ch.descabato.core

import java.io.File
import ch.descabato.utils.Utils

trait BackupException

case class PasswordWrongException(message: String, cause: Throwable) extends Exception(message, cause) with BackupException

case class BackupCorruptedException(file: File, repairTried: Boolean = false)
  extends Exception("File " + file + " of this backup is corrupt") with BackupException

class BackupInUseException extends Exception("Another process seems to be changing this backup.") with BackupException
  
case class MisconfigurationException(message: String) extends Exception(message) with BackupException

object ExceptionFactory {
  def newPar2Missing() = {
    var message = """Par2 command line utility is missing. It is needed if redundancy is enabled.
To disable the redundancy, add --no-redundancy to command line.
"""
    message += (if (Utils.isWindows) {
      """Otherwise please add the par2.exe in the bin folder, its parent folder or in a folder called tools.
Get it for example here: http://chuchusoft.com/par2_tbb/"""
    } else {
      "Otherwise please install par2 from the repositories."
    })
    new MisconfigurationException(message)
  }
}