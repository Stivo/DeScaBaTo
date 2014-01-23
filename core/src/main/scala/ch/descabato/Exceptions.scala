package ch.descabato

import java.io.File

trait BackupException

case class PasswordWrongException(message: String, cause: Throwable) extends Exception(message, cause) with BackupException

case class BackupCorruptedException(f: File) extends Exception("File "+f+" of this backup is corrupt") with BackupException