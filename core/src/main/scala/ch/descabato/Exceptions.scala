package ch.descabato

trait BackupException

case class PasswordWrongException(message: String, cause: Throwable) extends Exception(message, cause) with BackupException