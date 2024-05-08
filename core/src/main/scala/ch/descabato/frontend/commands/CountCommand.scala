package ch.descabato.frontend.commands

import ch.descabato.core.FileVisitorCollector
import ch.descabato.core.model.Size
import ch.descabato.frontend.CountConf
import ch.descabato.frontend.SizeStandardCounter
import ch.descabato.frontend.StandardMaxValueCounter
import ch.descabato.utils.Utils

class CountCommand extends Utils {

  def start(t: CountConf): Unit = {
    logger.info(t.summary)
    val fileCounter = new StandardMaxValueCounter("Files", 0)
    val bytesCounter = new SizeStandardCounter("Bytes")
    FileVisitorCollector.listFiles(t.ignoreFile.toOption, t.foldersToCountIn(), fileCounter, bytesCounter)
    logger.info(s"Would backup ${fileCounter.maxValue} files and ${Size(bytesCounter.maxValue)} bytes in total")
  }

}
