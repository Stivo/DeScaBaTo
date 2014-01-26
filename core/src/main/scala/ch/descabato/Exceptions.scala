package ch.descabato

import java.io.File

trait BackupException

case class PasswordWrongException(message: String, cause: Throwable) extends Exception(message, cause) with BackupException

case class BackupCorruptedException(file: File, repairTried: Boolean = false)
  extends Exception("File " + file + " of this backup is corrupt") with BackupException