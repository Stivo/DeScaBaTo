package ch.descabato.core.commands

import java.io.{File, FileOutputStream}
import java.util.Date

import akka.stream.scaladsl.Source
import akka.util.ByteString
import ch.descabato.core.Universe
import ch.descabato.frontend.RestoreConf
import ch.descabato.utils.{BytesWrapper, CompressedStream, Hash, Utils}
import ch.descabato.utils.Implicits._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class DoRestore(val universe: Universe) extends Utils {

  import universe.ex
  import universe.materializer

  private val config = universe.config

  def restore(options: RestoreConf, d: Option[Date] = None): Unit = {
    val startup = Await.result(universe.startup(), 1.minute)
    if (startup) {
      restoreImpl(options)
    }
    Await.result(universe.finish(), 1.hour)
  }

  private def restoreImpl(options: RestoreConf) = {
    val restoreDest = new File("restored")
    restoreDest.mkdirs()
    var finished: Boolean = false
    val eventualMetadatas = universe.backupFileActor.backedUpFiles()
    eventualMetadatas.foreach { files =>
      logger.info(s"Restoring ${files.size} files")
      for (file <- files) {
        val restoreDestination = new File(restoreDest, file.fd.path.replace(':', '_').replace('/', '_').replace('\\', '_'))
        restoreDestination.getParentFile.mkdirs()
        val stream = new FileOutputStream(restoreDestination)
        val source = Source.fromIterator[BytesWrapper](() => file.blocks.bytes.grouped(config.hashLength).map(_.wrap))
        Await.result(source.mapAsync(10) { hashBytes =>
          val hash = Hash(hashBytes.asArray())
          universe.blockStorageActor.read(hash)
        }.mapAsync(10) { bs =>
          Future(CompressedStream.decompressToBytes(bs))
        }.runForeach { x =>
          stream.write(x)
        }, 1.hour)
        stream.close()
        restoreDestination.setLastModified(file.fd.lastModified)
      }
      finished = true
    }
    while (!finished) {
      Thread.sleep(500)
    }

  }

}
