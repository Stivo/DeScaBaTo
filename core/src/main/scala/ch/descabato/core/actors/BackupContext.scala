package ch.descabato.core.actors

import java.io.File

import akka.actor.ActorSystem
import ch.descabato.core.util.FileWriter
import ch.descabato.core_old.{BackupFolderConfiguration, FileManager}
import ch.descabato.utils.Hash

import scala.concurrent.ExecutionContext

class BackupContext(val config: BackupFolderConfiguration,
                    val actorSystem: ActorSystem,
                    val fileManager: FileManager,
                    implicit val executionContext: ExecutionContext,
                    val eventBus: MyEventBus) {

  def sendFileFinishedEvent(fileWriter: FileWriter): Unit = {
    sendFileFinishedEvent(fileWriter.file, fileWriter.md5Hash())
  }

  def sendFileFinishedEvent(file: File, md5Hash: Hash): Unit = {
    val typ = fileManager.fileTypeForFile(file).get
    eventBus.publish(FileFinished(typ, file, false, md5Hash))
  }
}

