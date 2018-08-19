package ch.descabato.core.actors

import akka.actor.ActorSystem
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.util.FileManager

import scala.concurrent.ExecutionContext

class BackupContext(val config: BackupFolderConfiguration,
                    val actorSystem: ActorSystem,
                    val fileManager: FileManager,
                    implicit val executionContext: ExecutionContext,
                    val eventBus: MyEventBus) {

}

