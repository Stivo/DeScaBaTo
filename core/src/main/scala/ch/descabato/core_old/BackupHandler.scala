package ch.descabato.core_old

import java.io._
import java.nio.file.{Files, LinkOption, NoSuchFileException}
import java.security.DigestOutputStream
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import ch.descabato.akka.AkkaUniverse
import ch.descabato.frontend._
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Streams.{BlockOutputStream, VariableBlockOutputStream}
import ch.descabato.utils._
import org.apache.commons.compress.utils.IOUtils

import scala.collection.mutable
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.Await
import scala.language.reflectiveCalls
import scala.util.Random
import scala.concurrent.duration._

trait MeasureTime {
  var startTime = 0L

  def startMeasuring() {
    startTime = System.nanoTime()
  }

  def measuredTime(): String = {
    val time = System.nanoTime()
    format(time - startTime)
  }

  def format(millis: Long): String = {
    val seconds = millis / 1000 / 1000 / 1000
    val minutes = seconds / 60
    val hours = minutes / 60
    val days = hours / 24
    val add = if (days == 0) "" else days + " days "
    f"$add${hours % 24}%02d:${minutes % 60}%02d:${seconds % 60}%02d"
  }

}

trait BackupRelatedHandler {
  def universe: UniverseI

  def config: BackupFolderConfiguration = universe.config

  var threadNumber = 0

  class FileProgress extends MaxValueCounter() {
    val name: String = "Current file " + (config.synchronized {
      val copy = threadNumber
      threadNumber += 1
      copy
    })
    ProgressReporters.addCounter(this)
    var filename = ""

    override def formatted: String = filename
  }

  def statistics(x: BackupDescription): String = {
    val num = x.size
    val totalSize = new Size(x.files.map(_.size).sum)
    var out = f"$num%8d, $totalSize"
    //    if ((1 to 10) contains num) {
    //      out += "\n" + x.map(_.path).mkString("\n")
    //    }
    out
  }

  def getHashlistForFile(fd: FileDescription): Seq[Hash] = {
    if (fd.hasHashList) {
      universe.hashListHandler().getHashlist(fd.hash, fd.size)
    } else
      List(fd.hash)
  }


}

class BackupHandler(val universe: UniverseI) extends Utils with BackupRelatedHandler with BackupProgressReporting with MeasureTime {

  var counters: ThreadLocal[FileProgress] = new ThreadLocal[FileProgress]() {
    override def initialValue = new FileProgress()
  }

  val nameOfOperation = "Backing up"


  import universe._

  def backup(files: Seq[File]) {
    val backupFile = new File(config.folder, config.prefix + "backup.json")
    universe.remoteHandler().uploadFile(backupFile)
    config.folder.mkdirs()
    startMeasuring()
    universe.loadBlocking()
    l.info("Starting backup")
    ConsoleManager.startConsoleUpdater()
    // Walk tree and compile to do list
    ProgressReporters.activeCounters = List(scanCounter)
    val visitor = new OldIndexVisitor(backupPartHandler.loadBackup(None).asMap, config.ignoreFile,
      recordNew = true, recordUnchanged = true, progress = Some(scanCounter)
    ).walk(files)
    var (newDesc, unchangedDesc, deletedDesc) = (visitor.newDesc, visitor.unchangedDesc, visitor.deletedDesc)

    l.info("Counting of files done")
    l.info("New Files      : " + statistics(newDesc))
    l.info("Unchanged files: " + statistics(unchangedDesc))
    l.info("Deleted files  : " + statistics(deletedDesc))
    if (universe.journalHandler().isInconsistentBackup()) {
      def finished(fileDesc: FileDescription): Boolean = {
        if (!Hash.isDefined(fileDesc.hash))
          return false
        var blocks: Iterable[Hash] = List(fileDesc.hash)
        if (fileDesc.hasHashList) {
          if (!universe.hashListHandler().isPersisted(fileDesc.hash)) {
            return false
          }
          blocks = universe.hashListHandler().getHashlist(fileDesc.hash, config.hashLength)
        }
        for (hash <- blocks) {
          if (!universe.blockHandler().isPersisted(hash)) {
            return false
          }
        }
        true
      }

      val (finishedFiles, unfinished) = unchangedDesc.files.partition(finished)
      newDesc = newDesc.copy(files = newDesc.files ++ unfinished)
      unchangedDesc = unchangedDesc.copy(files = finishedFiles)
      l.info("Counting of files after considering inconsistent backup state")
      l.info("New Files      : " + statistics(newDesc))
      l.info("Unchanged files: " + statistics(unchangedDesc))
    }
    if ((newDesc.size + deletedDesc.size) == 0) {
      ProgressReporters.activeCounters = Nil
      if (universe.remoteHandler().remaining() == 0) {
        l.info("No files have been changed and remote is up to date")
        return
      } else {
        l.info("No files have been changed, but need to upload some missing files")
        ProgressReporters.openGui("Backup", false, config.remoteOptions)
        universe.finish()
        return
      }
    }
    journalHandler().startWriting()
    backupPartHandler.setFiles(unchangedDesc, newDesc)

    // Backup files 
    setMaximums(newDesc, withGui = true)
    ProgressReporters.activeCounters = List(fileCounter, byteCounter, failureCounter)
    universe.blockHandler.setTotalSize(byteCounter.maxValue)

    val coll = if (true) newDesc.files
    else {
      val out = newDesc.files.par
      val tp = new ForkJoinTaskSupport() {
        override def parallelismLevel = 2
      }
      out.tasksupport = tp
      out
    }

    val r = new Random()
    val (success, failed) = r.shuffle(newDesc.files).partition(backupFileDesc)
    universe.finish()
    journalHandler().stopWriting()
    ProgressReporters.activeCounters = Nil
    l.info("Successfully backed up " + success.size + ", failed " + failed.size)
    // Clean up, handle failed entries
    l.info("Backup completed in " + measuredTime())
  }


  def backupFileDesc(fileDesc: FileDescription): Boolean = {
    val byteCounterbackup = byteCounter.current
    counters.get.maxValue = fileDesc.size
    counters.get.current = 0
    counters.get.filename = fileDesc.name
    ProgressReporters.activeCounters = List(fileCounter, byteCounter, failureCounter, counters.get)
    var fis: FileInputStream = null
    var success = false
    try {
      // This is a new file, so we start hashing its contents, fill those in and return the same instance
      val file = new File(fileDesc.path)
      fis = new FileInputStream(file)
      //      if (config.renameDetection) {
      //        val oldOne = checkForRename(file, fileDesc, fis)
      //        for (old <- oldOne) {
      //          l.info("Detected rename from " + old.path + " to " + fileDesc.path)
      //          deletedCopy -= old
      //          val out = old.copy(path = fileDesc.path)
      //          out.hash = old.hash
      //          return Some(out)
      //        }
      //        fis.getChannel.position(0)
      //      }
      //lazy val compressionDisabled = compressionFor(fileDesc)
//      val blockHasher: OutputStream = createChunkerStream(fileDesc)
      val blockHasher: OutputStream = createVariableChunkerStream(fileDesc)
      IOUtils.copy(fis, blockHasher, config.blockSize.bytes.toInt * 2)
      IOUtils.closeQuietly(fis)
      IOUtils.closeQuietly(blockHasher)
      universe.hashFileHandler.finish(fileDesc)
      fileCounter += 1
      success = true
      true
    } catch {
      case io: IOException if io.getMessage().contains("The process cannot access the file") =>
        l.warn("Could not backup file " + fileDesc.path + " because it is locked.")
        false
      // TODO linux add case for symlinks
      case io: IOException if io.getMessage().contains("The system cannot find the") =>
        l.info(s"File ${fileDesc.path} has been deleted since starting the backup")
        false
      case io: FileNotFoundException if io.getMessage().contains("Access is denied") =>
        l.info(s"File ${fileDesc.path} can not be accessed")
        false
      case e: IOException => throw e
    } finally {
      if (fis != null)
        fis.close()
      if (!success) {
        l.info("File was not successfully backed up " + fileDesc)
        failureCounter += 1
        universe.hashFileHandler().fileFailed(fileDesc)
        universe.backupPartHandler().fileFailed(fileDesc)
      }
    }
  }

  private def createChunkerStream(fileDesc: FileDescription) = {
    val creator = new BlockCreator(fileDesc)
    new BlockOutputStream(config.blockSize.bytes.toInt, creator.blockArrived)
  }

  private def createVariableChunkerStream(fileDesc: FileDescription) = {
    val creator = new BlockCreator(fileDesc)
    new VariableBlockOutputStream(creator.blockArrived)
  }

  class BlockCreator(val fileDesc: FileDescription) {
    private var i = 0

    def blockArrived(block: BytesWrapper): Unit = {
      val bid = BlockId(fileDesc, i)
      i += 1
      val wrapper = new Block(bid, block)
      universe.scheduleTask { () =>
        val md = universe.config.createMessageDigest
        wrapper.hash = md.digest(wrapper.content)
        universe.backupPartHandler.hashComputed(wrapper)
      }
      universe.hashFileHandler.hash(wrapper)
      byteCounter += block.length
      counters.get += block.length
      waitForQueues()
      while (CLI.paused) {
        Thread.sleep(100)
      }
    }

  }

}


class RestoreHandler(val universe: UniverseI) extends Utils with BackupRelatedHandler with BackupProgressReporting with MeasureTime {

  val nameOfOperation = "Restoring"

  lazy val filecounter = new FileProgress()

  sealed trait Result

  case object IsSubFolder extends Result

  case object IsTopFolder extends Result

  case object IsUnrelated extends Result

  case object IsSame extends Result

  def isRelated(folder: BackupPart, relatedTo: BackupPart): Result = {
    var folderParts = folder.pathParts.toList
    var relatedParts = relatedTo.pathParts.toList
    while (relatedParts.nonEmpty && relatedParts.headOption == folderParts.headOption) {
      relatedParts = relatedParts.tail
      folderParts = folderParts.tail
    }
    if (folderParts.lengthCompare(folder.pathParts.size) == 0) {
      IsUnrelated
    } else {
      (folderParts, relatedParts) match {
        case (Nil, x :: _) => IsSubFolder
        case (x :: _, Nil) => IsTopFolder
        case (x :: _, y :: _) => IsUnrelated
        case _ => IsSame
      }
    }
  }

  def detectRoots(folders: Iterable[BackupPart]): Seq[FolderDescription] = {
    var candidates = mutable.Buffer[BackupPart]()
    folders.view.filter(_.isFolder).foreach {
      folder =>
        if (folder.path == "/") return List(folder.asInstanceOf[FolderDescription])
        // compare with each candidate
        var foundACandidate = false
        candidates = candidates.map {
          candidate =>
            // case1: This folder is a subfolder of candidate
            //		=> nothing changes, but search is aborted
            // case2: this folder is a parent of candidate
            //		=> this folder should replace that candidate, search is aborted
            // case3: this folder is not related to any candidate
            //		=> folder is added to candidates
            isRelated(candidate, folder) match {
              case IsUnrelated => candidate
              case IsTopFolder =>
                foundACandidate = true; folder
              case IsSubFolder =>
                foundACandidate = true; candidate
              case IsSame => throw new IllegalStateException("Duplicate entry found: " + folder + " " + candidate)
            }
        }.distinct
        if (!foundACandidate) {
          candidates += folder
        }
    }
    candidates.flatMap {
      case x: FolderDescription => List(x)
    }.toList
  }


  var roots: Iterable[FolderDescription] = mutable.Buffer.empty
  var relativeToRoot: Boolean = false

  def getRoot(sub: String): Option[FolderDescription] = {
    // TODO this asks for a refactoring
    val fd = FolderDescription(sub, null)
    roots.find {
      x => val r = isRelated(x, fd); r == IsSubFolder || r == IsSame
    }
  }

  def initRoots(folders: Seq[FolderDescription]) {
    roots = detectRoots(folders)
    relativeToRoot = roots.size == 1 || roots.map(_.name).toList.distinct.lengthCompare(roots.size) == 0
  }

  def writeRestoreInfo(description: BackupDescription)(implicit options: RestoreConf) {
    val format = new SimpleDateFormat()
    val backupFile = universe.backupPartHandler.loadedBackup.head
    val date = universe.fileManager().getFileType(backupFile).date(backupFile)
    val info =
      s"""Restored backup from ${format.format(date)} successfully, on ${format.format(new Date())}
         |Filename of restored file: $backupFile
         |Restored ${description.files.length} files with total size of ${Size(description.files.map(_.size).sum)}
         |in ${description.folders.size} folders""".stripMargin
    val lineEndings = if (Utils.isWindows) info.replace("\n", "\r\n") else info
    val file = if (options.restoreToFolder.isDefined) {
      new File(new File(options.restoreToFolder()), options.restoreInfo())
    } else {
      new File(options.restoreInfo())
    }
    val fos = new FileOutputStream(file)
    fos.write(lineEndings.getBytes())
    IOUtils.closeQuietly(fos)
  }

  def restore(options: RestoreConf, d: Option[Date] = None) {
    implicit val o: RestoreConf = options
    startMeasuring()
    val description = universe.backupPartHandler().loadBackup(d)
    initRoots(description.folders)
    val filtered = description // TODO if (o.pattern.isDefined) filesInBackup.filter(x => x.path.contains(options.pattern())) else filesInBackup
    l.info("Going to restore " + statistics(filtered))
    setMaximums(filtered, withGui = true)
    ProgressReporters.activeCounters = List(fileCounter, byteCounter, failureCounter)
    universe.loadBlocking()
    description.folders.foreach(restoreFolderDesc(_))
    description.files.foreach(restoreFileDesc)
    description.symlinks.foreach(restoreLink)
    description.folders.foreach(restoreFolderDesc(_, count = false))
    if (options.restoreInfo.isDefined) {
      writeRestoreInfo(description)
    }
    universe.finish()
    ProgressReporters.activeCounters = Nil
    println("Finished restoring " + measuredTime())
  }

  def restoreFromDate(t: RestoreConf, d: Date) {
    restore(t, Some(d))
  }

  def makePath(path: String, maybeOutside: Boolean = false)(implicit options: RestoreConf): File = {
    if (options.restoreToOriginalPath()) {
      return new File(path)
    }

    def cleaned(s: String) = if (Utils.isWindows) s.replaceAllLiterally(":", "_") else s

    val dest = new File(options.restoreToFolder())
    if (relativeToRoot) {
      val relativeTo = getRoot(path) match {
        case Some(x: BackupPart) => x.path
        // this is outside, so we return the path
        case None if !maybeOutside => throw new IllegalStateException("This should be inside the roots, some error")
        case None => return new File(path)
      }
      FileUtils.getRelativePath(dest, new File(relativeTo), path)
    } else {
      new File(dest, cleaned(path))
    }
  }

  def restoreLink(link: SymbolicLink)(implicit options: RestoreConf) {
    try {
      val path = makePath(link.path)
      if (Files.exists(path.toPath(), LinkOption.NOFOLLOW_LINKS))
        return
      val linkTarget = makePath(link.linkTarget, maybeOutside = true)
      //println("Creating link from " + path + " to " + linkTarget)
      Files.createSymbolicLink(path.toPath(), linkTarget.getCanonicalFile().toPath())
      link.applyAttrsTo(path)
    } catch {
      case e@(_: UnsupportedOperationException | _: NoSuchFileException) =>
        l.warn("Symbolic link to " + link.linkTarget + " can not be restored")
        logException(e)
    }
  }

  def restoreFolderDesc(fd: FolderDescription, count: Boolean = true)(implicit options: RestoreConf) {
    val restoredFile = makePath(fd.path)
    restoredFile.mkdirs()
    fd.applyAttrsTo(restoredFile)
    if (count)
      fileCounter += 1
  }

  def restoreFileDesc(fd: FileDescription)(implicit options: RestoreConf) {
    try {
      val restoredFile = makePath(fd.path)
      if (restoredFile.exists()) {
        if (restoredFile.length() == fd.size && !fd.attrs.hasBeenModified(restoredFile)) {
          fileCounter += 1
          byteCounter += fd.size
          return
        }
        l.debug(s"${restoredFile.length()} ${fd.size} ${fd.attrs} ${restoredFile.lastModified()}")
        // TODO add option
        l.info("File exists, but has been modified, so overwrite")
      }
      if (!restoredFile.getParentFile().exists())
        restoredFile.getParentFile().mkdirs()

      filecounter.filename = restoredFile.getName
      filecounter.maxValue = fd.size
      filecounter.current = 0
      val actor = universe.createRestoreHandler(fd, restoredFile, filecounter)
      val future = actor.restore()
      Await.result(future, 24.hours)
      // TODO add these again
      //      val hash = dos.getMessageDigest().digest()
      //      if (!util.Arrays.equals(fd.hash, hash)) {
      //        l.warn("Error while restoring file, hash is not correct")
      //      }
      //      if (restoredFile.length() != fd.size) {
      //        l.warn(s"Restoring failed for $restoredFile (old size: ${fd.size}, now: ${restoredFile.length()}")
      //      }
      fd.applyAttrsTo(restoredFile)
    } catch {
      case e@BackupCorruptedException(f, false) =>
        universe.finish()
        throw e
      case e: IOException if e.getMessage.toLowerCase.contains("no space left") =>
        throw e
      case e: Exception =>
        universe.finish()
        l.warn("Exception while restoring " + fd.path + " (" + e.getMessage() + ")")
        logException(e)
    }
    fileCounter += 1
    byteCounter += fd.size
  }

}

class ProblemCounter extends Counter {
  def name = "Problems"
}

class VerifyHandler(val universe: UniverseI)
  extends BackupRelatedHandler with Utils with BackupProgressReporting with MeasureTime {

  val nameOfOperation = "Verifying"

  var problemCounter = new ProblemCounter()

  lazy val backupDesc: BackupDescription = universe.backupPartHandler().loadBackup()

  def verify(t: VerifyConf): Long = {
    startMeasuring()
    problemCounter = new ProblemCounter()
    universe.load()
    try {
      universe.blockHandler().verify(problemCounter)
      verifyHashes()
      verifySomeFiles(t.percentOfFilesToCheck())
      universe.finish()
    } catch {
      case x: Error =>
        problemCounter += 1
        l.warn("Aborting verification because of error ", x)
        logException(x)
    }
    l.info(s"Verification completed with ${problemCounter.current} errors in " + measuredTime())
    problemCounter.current
  }

  def verifyHashes() {
    val it = backupDesc.files.iterator
    while (it.hasNext) {
      it.next match {
        case f: FileDescription =>
          val chain = getHashlistForFile(f)
          val missing = chain.filterNot {
            universe.blockHandler().isPersisted
          }
          if (missing.nonEmpty) {
            problemCounter += missing.size
            l.warn(s"Missing ${missing.size} blocks for $f")
          }
        case _ =>
      }
    }
  }

  val random = new Random()

  def getRandomXElements[T](x: Int, array: Array[T]): Array[T] = {
    def swap(i: Int, i2: Int) {
      val bak = array(i)
      array(i) = array(i2)
      array(i2) = bak
    }

    val end = Math.min(x, array.length - 2) + 1
    (0 to end).foreach { i =>
      val chooseFrom = array.length - 1 - i
      if (chooseFrom > 0)
        swap(i, random.nextInt(chooseFrom) + i)
    }
    array.take(end)
  }

  def verifySomeFiles(percent: Int) {
    ProgressReporters.activeCounters = List(fileCounter, byteCounter)
    val files = backupDesc.files.toArray
    var probes = ((percent * 1.0 / 100.0) * files.length).toInt + 1
    if (probes > files.length)
      probes = files.length
    if (probes <= 0)
      return
    val tests = getRandomXElements(probes, files)
    setMaximums(tests, withGui = true)
    tests.foreach { file =>
      val hashList = getHashlistForFile(file)
      val md = config.createMessageDigest()
      for (blockHash <- hashList) {
        val bytes = getChunk(blockHash)
        md.update(bytes)
        byteCounter += bytes.length
      }
      val hash = new Hash(md.digest())
      if (!(file.hash === hash)) {
        l.warn("File " + file + " is broken in backup")
        l.warn(file.hash.base64 + " " + hash.base64)
        problemCounter += 1
      }
      fileCounter += 1
    }
    ProgressReporters.activeCounters = Nil
  }

  def getChunk(blockHash: Hash): BytesWrapper = {
    val compressed = universe.blockHandler().readBlock(blockHash)
    val ret = CompressedStream.decompressToBytes(compressed)
    // TODO fix this
    //    if (config.getMessageDigest().digest(ret) safeEquals blockHash) {
    //    }
    ret
  }

}
