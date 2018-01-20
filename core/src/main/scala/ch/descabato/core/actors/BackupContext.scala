package ch.descabato.core.actors

import ch.descabato.core_old.{BackupFolderConfiguration, FileManager}

import scala.concurrent.ExecutionContext

class BackupContext(val config: BackupFolderConfiguration,
                    val fileManager: FileManager,
                    implicit val executionContext: ExecutionContext)
