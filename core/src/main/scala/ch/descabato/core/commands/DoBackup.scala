package ch.descabato.core.commands

import java.io.File
import java.nio.file.Path

import akka.Done
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.{ClosedShape, OverflowStrategy}
import ch.descabato.core._
import ch.descabato.core.actors.Chunker
import ch.descabato.core.model._
import ch.descabato.core_old.{FileDescription, FolderDescription}
import ch.descabato.frontend._
import ch.descabato.utils.Implicits._
import ch.descabato.utils.{BytesWrapper, Hash, MeasureTime, Utils}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class DoBackup(val universe: Universe, val foldersToBackup: Seq[File]) extends Utils with MeasureTime {

  val config = universe.config

  implicit val executionContext = universe.ex
  implicit val materializer = universe.materializer

  var fileCounter: MaxValueCounter = new StandardMaxValueCounter("Files", 0L)
    with ETACounter

  lazy val bytesCounter: SizeStandardCounter = new SizeStandardCounter("Data Read") with ETACounter

  ProgressReporters.addCounter(fileCounter, bytesCounter)

  private def gatherFiles() = {
    Future {
      val collector = new FileVisitorCollector(config.ignoreFile, fileCounter, bytesCounter)
      foldersToBackup.foreach { folder =>
        collector.walk(folder.toPath)
      }
      collector
    }
  }

  def backupAlreadyBackedUpFiles(x: Path, knownFiles: Map[String, FileMetadataStored]): Boolean = {
    knownFiles.get(x.toFile.getAbsolutePath).map { fileStored =>
      val out = fileStored.checkIfMatch(new FileDescription(x.toFile))
      if (out) {
        fileCounter.maxValue += -1
        bytesCounter.maxValue += -fileStored.fd.size
        universe.metadataStorageActor.saveFileSameAsBefore(fileStored)
      }
      out
    }.getOrElse(false)
  }

  def execute(): Unit = {
    startMeasuring()
    ProgressReporters.openGui("Backup", false)
    universe.journalHandler.cleanUnfinishedFiles()
    val lastWasInconsistent = universe.journalHandler.isInconsistentBackup()
    val universeStartup = universe.startup()
    universe.journalHandler.startWriting()
    val fileGathering = gatherFiles()
    val universeStarted = Await.result(universeStartup, 1.minute)
    val fileCollector = Await.result(fileGathering, 1.minute)
    logger.info(s"${fileCounter.maxValue} files collected and program is ready after ${measuredTime()}")
    ProgressReporters.activeCounters = List(fileCounter, bytesCounter)
    if (universeStarted) {
      if (!lastWasInconsistent) {
        val knownFiles = universe.metadataStorageActor.getKnownFiles()
        val (_, newFiles) = fileCollector.files.par.partition(x => backupAlreadyBackedUpFiles(x, knownFiles))
        Await.result(setupFlow(newFiles.seq), Duration.Inf)
      } else {
        Await.result(setupFlow(fileCollector.files), Duration.Inf)
      }
    }
    logger.info(s"Files are backed up after ${measuredTime()}, saving ${fileCollector.dirs.size} directories")
    saveDirectories(fileCollector.dirs)
    logger.info(s"Backup is done, waiting for writing to finish after ${measuredTime()}")
    Await.result(universe.finish(), 5.minutes)
    universe.compressor.report()
    universe.journalHandler.stopWriting()
    logger.info(s"Backup is completely done after ${measuredTime()}")
  }

  private def setupFlow(files: Seq[Path]) = {
    Source.fromIterator[Path](() => files.iterator).mapAsync(2) { path =>
      backupFile(path)
        .map(x => (path, x))
    }.map { case (path, _) =>
      fileCounter += 1
      bytesCounter += path.toFile.length()
    }.runWith(Sink.ignore)
  }

  //  def streamCounter[T](name: String) = Flow[T].zipWithIndex.map { case (x, i) =>
  //    logger.info(s"$path $name: Element $i")
  //    x
  //  }

  private def saveDirectories(dirs: Seq[Path]) = {
    val dirsSaved: Future[Done] = Source.fromIterator[Path](() => dirs.iterator).mapAsync(5) { path =>
      universe.metadataStorageActor.addDirectory(FolderDescription(path.toFile))
    }.runWith(Sink.ignore)
    Await.result(dirsSaved, 1.hours)
  }

  def backupFile(path: Path): Future[Done] = {
    val fd = new FileDescription(path.toFile)
    universe.metadataStorageActor.hasAlready(fd).flatMap {
      case FileNotYetBackedUp =>
        hashAndBackupFile(path, fd)
      case FileAlreadyBackedUp(metadata) =>
        Source.fromIterator[Long](() => metadata.chunkIds.iterator).mapAsync(5) { id =>
          universe.chunkStorageActor.hasAlready(id)
        }.fold(true)(_ && _).mapAsync(1) { hasAll =>
          if (hasAll) {
            bytesCounter.maxValue -= fd.size
            bytesCounter += -fd.size
            universe.metadataStorageActor.saveFileSameAsBefore(metadata).map(_ => Done)
          } else {
            logger.info(s"File $fd is saved but some blocks are missing, so backing up again")
            hashAndBackupFile(path, fd)
          }
        }.runWith(Sink.ignore)
      case Storing =>
        logger.warn(s"Seems we already are saving $fd (symlink?)")
        Future.successful(Done)
    }
  }

  private def hashAndBackupFile(path: Path, fd: FileDescription): Future[Done] = {
    val graph = RunnableGraph.fromGraph(GraphDSL.create(Sink.ignore) { implicit builder =>
      sink =>
        import GraphDSL.Implicits._
        val chunkSize = config.blockSize.bytes.toInt
        val chunker = new Chunker()
        val chunkSource = FileIO.fromPath(path, chunkSize = 256 * 1024) ~> chunker

        val hashedBlocks = builder.add(Broadcast[(Block, ChunkIdResult)](2))
        val waitForCompletion = builder.add(Merge[Unit](2))

        chunkSource ~> hashAndCreateBlocks(fd) ~> newBuffer[Block](20) ~> assignIds ~> hashedBlocks

        hashedBlocks ~> createCompressAndSaveBlocks(fd) ~> waitForCompletion.in(0)
        hashedBlocks ~> gatherHashIdsAndSave(fd) ~> waitForCompletion.in(1)

        waitForCompletion.out ~> sink
        ClosedShape
    })
    graph.run()
  }

  private def mapToUnit() = Flow[Any].map(_ => ())

  private def hashAndCreateBlocks(fd: FileDescription) = Flow[BytesWrapper].zipWithIndex.mapAsync(10) { case (x, i) =>
    Future {
      val digest = config.createMessageDigest()
      digest.update(x)
      val hash = Hash(digest.digest())
      Block(BlockId(fd, i.toInt), x, hash)
    }
  }

  private val sendToChunkStorage = Flow[(CompressedBlock, Long)].mapAsync(5) { case (b, i) =>
    universe.chunkStorageActor.save(b, i)
  }

  private def newBuffer[T](bufferSize: Int = 100) = Flow[T].buffer(bufferSize, OverflowStrategy.backpressure)

  private val assignIds = Flow[Block].mapAsync(5) { x =>
    universe.chunkStorageActor.chunkId(x, true).map { fut =>
      (x, fut)
    }
  }

  private val filterNonExistingBlocksAndCompress = Flow[(Block, ChunkIdResult)]
    .collect {
      case (block, ChunkIdAssigned(id)) => (block, id)
    }.mapAsync(8) { case (block, id) => universe.compressor.compressBlock(block).map(x => (x, id)) }

  private def createCompressAndSaveBlocks(fd: FileDescription) = {
    filterNonExistingBlocksAndCompress.via(newBuffer[(CompressedBlock, Long)](20)).via(sendToChunkStorage).via(mapToUnit())
  }

  private def createFileMetadataAndSave(fd: FileDescription) = Flow[Seq[Long]].mapAsync(2) { hashes =>
    universe.metadataStorageActor.saveFile(fd, hashes)
  }

  private val gatherIds = Flow[(Block, ChunkIdResult)].map(_._2).map {
    case ChunkIdAssigned(id) => id
    case ChunkFound(id) => id
  }.fold(Vector.empty[Long])(_ :+ _)

  private def gatherHashIdsAndSave(fd: FileDescription) = {
    val createFileDescription = createFileMetadataAndSave(fd)
    gatherIds.via(createFileDescription).via(mapToUnit())
  }
}
