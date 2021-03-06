package ch.descabato.core.commands

import java.nio.file.Paths
import java.util.Date

import ch.descabato.core.Universe
import ch.descabato.core.actors.MetadataStorageActor.BackupDescription
import ch.descabato.core.model.FileMetadataStored
import ch.descabato.utils.Implicits._
import ch.descabato.utils._

import scala.concurrent.Await
import scala.concurrent.duration._

class DoVerify(_universe: Universe) extends DoReadAbstract(_universe) with Utils {

  import universe.materializer

  def verifyAllBackedupIdsHaveMetadata(date: Date, counter: ProblemCounter) = {
    universe.metadataStorageActor.verifyMetadataForIdsAvailable(date, counter)
  }

  def verifyAll(): ProblemCounter = {
    waitForStartup()
    // verify backup:
    val counter = new ProblemCounter()
    //  - check that all metadata for all files and directories are available
    val files = universe.fileManagerNew.backup.getDates()
    for (file <- files) {
      verifyAllBackedupIdsHaveMetadata(file, counter)
    }
    //  - check that all chunks of all files are in chunk storage, and in volume
    val chunkIdsToTest = universe.metadataStorageActor.getAllFileChunkIds()
    universe.chunkStorageActor.verifyChunksAreAvailable(chunkIdsToTest, counter, checkVolumeToo = true, checkContent = true)
    logger.info(s"Finished verifying, found ${counter.count} problems")
    counter
  }

  def verifyDate(date: Date, percentageToVerify: Double = 1): ProblemCounter = {
    verify(Some(date), percentageToVerify)
  }

  def verify(date: Option[Date] = None, percentageToVerify: Double = 1): ProblemCounter = {
    val startup = Await.result(universe.startup(), 1.minute)
    val metadata = Await.result(universe.metadataStorageActor.retrieveBackup(date), 1.minute)
    val counter = new ProblemCounter()
    if (startup) {
      verifyImpl(metadata, percentageToVerify, counter)
    }
    counter
  }

  private def verifyImpl(metadata: BackupDescription, percentageToVerify: Double, counter: ProblemCounter) = {
    require(0 <= percentageToVerify && percentageToVerify <= 1, "Percentage must be between 0 and 1")
    if (percentageToVerify < 1) {
      ???
    }
    for (file <- metadata.files) {
      verifyFileIsSaved(file, metadata, counter)
    }
  }

  private def verifyFileIsSaved(file: FileMetadataStored, metadata: BackupDescription, counter: ProblemCounter) = {
    checkParentIsSaved(file, metadata, counter)
    checkContentIsSaved(file, metadata, counter)
  }

  def checkContentIsSaved(file: FileMetadataStored, metadata: BackupDescription, counter: ProblemCounter) = {
    val value = chunkIdsForFile(file)
    val hashes = value.mapAsync(2)(id => universe.chunkStorageActor.getHashForId(id))
    val bytestream = getBytestream(value)
    val withHashes = bytestream.zip(hashes).zipWithIndex
    val future = withHashes.runForeach { case ((bytes, savedHash), index) =>
      val calculatedHash = config.createMessageDigest().digest(bytes)
      if (calculatedHash !== Hash(savedHash)) {
        counter.addProblem(s"File $file has invalid block number $index")
      }
    }
    Await.result(future, 1.hours)
  }

  private def checkParentIsSaved(file: FileMetadataStored, metadata: BackupDescription, counter: ProblemCounter) = {
    val parent = Paths.get(file.fd.path).getParent
    if (metadata.folders.find(x => Paths.get(x.path) == parent).isEmpty) {
      counter.addProblem(s"Could not find folder for ${file.fd}")
    }
  }
}

class ProblemCounter {

  private var _problems: Seq[String] = Seq.empty

  def addProblem(description: String): Unit = {
    this.synchronized {
      _problems :+= description
      println(s"$description (now at $count problems)")
    }
  }

  def count: Long = _problems.size

  def problems: Seq[String] = _problems
}