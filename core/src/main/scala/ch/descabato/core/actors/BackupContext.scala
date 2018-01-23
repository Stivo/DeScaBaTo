package ch.descabato.core.actors

import java.io.File

import akka.actor.ActorSystem
import ch.descabato.core_old.{BackupFolderConfiguration, FileManager}

import scala.concurrent.ExecutionContext

class BackupContext(val config: BackupFolderConfiguration,
                    val actorSystem: ActorSystem,
                    val fileManager: FileManager,
                    implicit val executionContext: ExecutionContext,
                    val eventBus: MyEventBus) {

  def sendFileFinishedEvent(file: File): Unit = {
    val typ = fileManager.getFileType(file)
    val isTempFile = typ.isTemp(file)
    eventBus.publish(FileFinished(typ, file, isTempFile))
  }
}

