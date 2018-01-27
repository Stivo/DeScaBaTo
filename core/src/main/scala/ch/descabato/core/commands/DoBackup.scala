package ch.descabato.core.commands

import java.io.File
import java.nio.file.{Files, Path}

import akka.Done
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.{ClosedShape, OverflowStrategy}
import ch.descabato.core._
import ch.descabato.core.actors.Chunker
import ch.descabato.core.model._
import ch.descabato.core_old.{FileDescription, FolderDescription, MeasureTime}
import ch.descabato.frontend.{ETACounter, MaxValueCounter, ProgressReporters, StandardMaxValueCounter}
import ch.descabato.utils.Implicits._
import ch.descabato.utils.{BytesWrapper, Hash, Utils}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class DoBackup(val universe: Universe, val foldersToBackup: Seq[File]) extends Utils with MeasureTime {

  val config = universe.config

  implicit val executionContext = universe.ex
  implicit val materializer = universe.materializer

  var fileCounter: MaxValueCounter = new StandardMaxValueCounter("Files", 0L)
    with ETACounter

  lazy val bytesCounter: StandardMaxValueCounter with ETACounter = new StandardMaxValueCounter("Data Read", 0) with ETACounter {
    override def formatted = s"${readableFileSize(current)}/${readableFileSize(maxValue)} $percent%"
  }

  ProgressReporters.addCounter(fileCounter, bytesCounter)

  private def gatherFiles() = {
    Future {
      val collector = new FileVisitorCollector(fileCounter, bytesCounter)
      foldersToBackup.foreach { folder =>
        Files.walkFileTree(folder.toPath, collector)
      }
      collector
    }
  }

  def execute(): Unit = {
    startMeasuring()
    ProgressReporters.openGui("Backup", false)
    universe.journalHandler.cleanUnfinishedFiles()
    val universeStartup = universe.startup()
    universe.journalHandler.startWriting()
    val fileGathering = gatherFiles()
    val universeStarted = Await.result(universeStartup, 1.minute)
    val fileCollector = Await.result(fileGathering, 1.minute)
    logger.info(s"${fileCounter.maxValue} files collected and program is ready after ${measuredTime()}")
    ProgressReporters.activeCounters = List(fileCounter, bytesCounter)
    if (universeStarted) {
      Await.result(setupFlow(fileCollector.files), Duration.Inf)
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
    Source.fromIterator[Path](() => files.iterator).mapAsync(5) { path =>
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
      universe.backupFileActor.addDirectory(FolderDescription(path.toFile))
    }.runWith(Sink.ignore)
    Await.result(dirsSaved, 1.hours)
  }

  def backupFile(path: Path): Future[Done] = {
    val fd = new FileDescription(path.toFile)
    universe.backupFileActor.hasAlready(fd).flatMap {
      case FileNotYetBackedUp =>
        hashAndBackupFile(path, fd)
      case FileAlreadyBackedUp(metadata) =>
        Source.fromIterator[Array[Byte]](() => metadata.blocks.grouped(config)).mapAsync(5) { bytes =>
          universe.blockStorageActor.hasAlready(Hash(bytes))
        }.fold(true)(_ && _).mapAsync(1) { hasAll =>
          if (hasAll) {
            bytesCounter.maxValue -= fd.size
            bytesCounter += -fd.size
            universe.backupFileActor.saveFileSameAsBefore(fd).map(_ => Done)
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

        val hashedBlocks = builder.add(Broadcast[Block](2))
        val waitForCompletion = builder.add(Merge[Unit](2))

        chunkSource ~> hashAndCreateBlocks(fd) ~> newBuffer[Block](20) ~> hashedBlocks

        hashedBlocks ~> createCompressAndSaveBlocks(fd) ~> waitForCompletion.in(0)
        hashedBlocks ~> createHashlistAndFileMetadataAndSave(fd) ~> waitForCompletion.in(1)

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

  private val sendToBlockIndex = Flow[Block].mapAsync(5) { b =>
    universe.blockStorageActor.save(b)
  }

  private def newBuffer[T](bufferSize: Int = 100) = Flow[T].buffer(bufferSize, OverflowStrategy.backpressure)

  private val filterNonExistingBlocksAndCompress = Flow[Block].mapAsync(5) { x =>
    universe.blockStorageActor.hasAlready(x).map { fut =>
      (x, fut)
    }
  }.filter(!_._2).map(_._1).mapAsync(8)(block => universe.compressor.compressBlock(block))

  private def createCompressAndSaveBlocks(fd: FileDescription) = {
    filterNonExistingBlocksAndCompress.via(newBuffer[Block](20)).via(sendToBlockIndex).via(mapToUnit())
  }

  private def createFileMetadataAndSave(fd: FileDescription) = Flow[Seq[Hash]].mapAsync(2) { hashes =>
    Future {
      val length = config.hashLength
      val hashlist = Array.ofDim[Byte](hashes.size * length)
      hashes.zipWithIndex.foreach {
        case (bytes, i) => System.arraycopy(bytes.bytes, 0, hashlist, length * i, length)
      }
      (fd, Hash(hashlist))
    }.flatMap { case (fd, hash) =>
      universe.backupFileActor.saveFile(fd, hash)
    }
  }

  private val concatHashes = Flow[Block].map(x => Seq(x.hash)).fold(Seq.empty[Hash])(_ ++ _)

  private def createHashlistAndFileMetadataAndSave(fd: FileDescription) = {
    val createFileDescription = createFileMetadataAndSave(fd)
    concatHashes.via(createFileDescription).via(mapToUnit())
  }
}
