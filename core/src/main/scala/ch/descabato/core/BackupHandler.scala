package ch.descabato.core

import java.io._
import ch.descabato.utils.{FileUtils, Utils}
import ch.descabato.utils.Streams._
import ch.descabato.frontend._
import scala.collection.parallel.ThreadPoolTaskSupport
import scala.collection.mutable
import java.util.Date
import java.nio.file.{NoSuchFileException, LinkOption, Files}
import java.security.DigestOutputStream
import java.util
import java.util.Enumeration
import scala.Some
import scala.util.Random

trait MeasureTime {
  var startTime = 0L
  def startMeasuring() {
    startTime = System.nanoTime()
  }

  def measuredTime() = {
    val time = System.nanoTime()
    format(time - startTime)
  }

  def format(millis: Long) = {
    val seconds = millis / 1000 / 1000 / 1000
    val minutes = seconds / 60
    val hours = minutes / 60
    val days = hours / 24
    val add = if (days == 0) "" else days + " days "
    f"$add${hours % 24}%02d:${minutes % 60}%02d:${seconds % 60}%02d"
  }

}

trait BackupRelatedHandler {
  def universe: Universe
  def config: BackupFolderConfiguration = universe.config

  def statistics(x: BackupDescription) = {
    val num = x.size
    val totalSize = new Size(x.files.map(_.size).sum)
    var out = f"$num%8d, $totalSize"
    //    if ((1 to 10) contains num) {
    //      out += "\n" + x.map(_.path).mkString("\n")
    //    }
    out
  }

  def getHashlistForFile(fd: FileDescription): Seq[Array[Byte]] = {
    if (fd.size > universe.config.blockSize.bytes) {
      universe.hashListHandler().getHashlist(fd.hash, fd.size)
    } else
      List(fd.hash)
  }

  def readBlock(hash: Array[Byte], verify: Boolean = false) = universe.blockHandler().readBlock(hash, verify)

  def getInputStream(fd: FileDescription): InputStream = {
    val hashes = getHashlistForFile(fd)
    val enumeration = new Enumeration[InputStream]() {
      val hashIterator = hashes.iterator
      def hasMoreElements = hashIterator.hasNext
      def nextElement = {
        try {
          readBlock(hashIterator.next, true)
        } catch {
          case x: Exception =>
            l.info("Exception while getting next element in stream",x)
          throw new Throwable()
        }
      }
    }
    new SequenceInputStream(enumeration)
  }
}

class BackupHandler(val universe: Universe) extends Utils with BackupRelatedHandler with BackupProgressReporting with MeasureTime {

  var counters = new ThreadLocal[FileProgress]() {
    override def initialValue = new FileProgress()
  }

  var threadNumber = 0

  val nameOfOperation = "Backing up"

  class FileProgress extends MaxValueCounter() {
    val name = "Current file " + (config.synchronized {
      val copy = threadNumber
      threadNumber += 1
      copy
    })
    ProgressReporters.addCounter(this)
    var filename = ""

    override def formatted = filename
  }

  import universe._

  def backup(files: Seq[File]) {
    config.folder.mkdirs()
    startMeasuring()
    universe.loadBlocking()
    l.info("Starting backup")
    ConsoleManager.startConsoleUpdater()
    // Walk tree and compile to do list
    ProgressReporters.activeCounters = List(scanCounter)
    val visitor = new OldIndexVisitor(backupPartHandler.loadBackup(None).asMap, recordNew = true, recordUnchanged = true, progress = Some(scanCounter)).walk(files)
    val (newDesc, unchangedDesc, deletedDesc) = (visitor.newDesc, visitor.unchangedDesc, visitor.deletedDesc)

    l.info("Counting of files done")
    l.info("New Files      : " + statistics(newDesc))
    l.info("Unchanged files: " + statistics(unchangedDesc))
    l.info("Deleted files  : " + statistics(deletedDesc))
    if ((newDesc.size + deletedDesc.size) == 0) {
      l.info("No files have been changed, aborting backup")
      return
    }
    val allFiles = unchangedDesc.merge(newDesc)
    backupPartHandler.setFiles(allFiles)

    // Backup files 
    setMaximums(newDesc)
    ProgressReporters.activeCounters = List(fileCounter, byteCounter)
    universe.blockHandler.setTotalSize(byteCounter.maxValue)

    val coll = if (true) newDesc.files
    else {
      val out = newDesc.files.par
      val tp = new ThreadPoolTaskSupport() {
        override def parallelismLevel = 2
      }
      out.tasksupport = tp
      out
    }

    val (success, failed) = coll.partition(backupFileDesc)
    universe.finish()
    l.info("Successfully backed up " + success.size + ", failed " + failed.size)
    // Clean up, handle failed entries
    l.info("Backup completed in " + measuredTime())
  }


  def backupFileDesc(fileDesc: FileDescription): Boolean = {
    val byteCounterbackup = byteCounter.current
    counters.get.maxValue = fileDesc.size
    counters.get.current = 0
    counters.get.filename = fileDesc.name
    ProgressReporters.activeCounters = List(fileCounter, byteCounter, counters.get)
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
      var i = 0
      val blockHasher = new BlockOutputStream(config.blockSize.bytes.toInt, {
        block: Array[Byte] =>
          val bid = new BlockId(fileDesc, i)
          universe.cpuTaskHandler.computeHash(block, config.hashAlgorithm, bid)
          universe.hashHandler.hash(bid, block)
          byteCounter += block.length
          counters.get += block.length
          waitForQueues()
          while (CLI.paused) {
            Thread.sleep(100)
          }
          i += 1
      })
      copy(fis, blockHasher)
      universe.hashHandler.finish(fileDesc)
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
        l.info("File was not successfully backed up "+fileDesc)
        universe.hashHandler().fileFailed(fileDesc)
      }
    }
  }

}

class RestoreHandler(val universe: Universe) extends Utils with BackupRelatedHandler with BackupProgressReporting with MeasureTime {

  val nameOfOperation = "Restoring"

  sealed trait Result

  case object IsSubFolder extends Result
  case object IsTopFolder extends Result
  case object IsUnrelated extends Result
  case object IsSame extends Result

  def isRelated(folder: BackupPart, relatedTo: BackupPart): Result = {
    var folderParts = folder.pathParts.toList
    var relatedParts = relatedTo.pathParts.toList
    while (relatedParts.headOption.isDefined && relatedParts.headOption == folderParts.headOption) {
      relatedParts = relatedParts.tail
      folderParts = folderParts.tail
    }
    if (folderParts.size == folder.pathParts.size) {
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

  def getRoot(sub: String) = {
    // TODO this asks for a refactoring
    val fd = new FolderDescription(sub, null)
    roots.find {
      x => val r = isRelated(x, fd); r == IsSubFolder || r == IsSame
    }
  }

  def initRoots(folders: Seq[FolderDescription]) {
    roots = detectRoots(folders)
    relativeToRoot = roots.size == 1 || roots.map(_.name).toList.distinct.size == roots.size
  }

  def restore(options: RestoreConf, d: Option[Date] = None) {
    implicit val o = options
    startMeasuring()
    val description = universe.backupPartHandler().loadBackup(d)
    initRoots(description.folders)
    val filtered = description // TODO if (o.pattern.isDefined) filesInBackup.filter(x => x.path.contains(options.pattern())) else filesInBackup
    l.info("Going to restore " + statistics(filtered))
    setMaximums(filtered)

    description.folders.foreach(restoreFolderDesc(_))
    description.files.par.foreach(restoreFileDesc)
    description.symlinks.foreach(restoreLink)
    description.folders.foreach(restoreFolderDesc(_, false))
    universe.finish()
    println("Finished restoring "+measuredTime())
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
      val linkTarget = makePath(link.linkTarget, true)
      println("Creating link from " + path + " to " + linkTarget)
      // TODO if link links into backup, path should be updated
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
    var closeAbles = mutable.Buffer[ {def close(): Unit}]()
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
      val fos = new UnclosedFileOutputStream(restoredFile)
      closeAbles += fos
      val dos = new DigestOutputStream(fos, config.getMessageDigest)
      closeAbles += dos
      val in = getInputStream(fd)
      closeAbles += in
      copy(in, dos)
      val hash = dos.getMessageDigest().digest()
      if (!util.Arrays.equals(fd.hash, hash)) {
        l.warn("Error while restoring file, hash is not correct")
      }
      if (restoredFile.length() != fd.size) {
        l.warn(s"Restoring failed for $restoredFile (old size: ${fd.size}, now: ${restoredFile.length()}")
      }
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
    } finally {
      closeAbles.reverse.foreach(_.close)
    }
    fileCounter += 1
    byteCounter += fd.size
  }

}

class ProblemCounter extends Counter {
  def name = "Problems"
}

class VerifyHandler(val universe: Universe)
  extends BackupRelatedHandler with Utils with BackupProgressReporting with MeasureTime {

  val nameOfOperation = "Verifying"

  var problemCounter = new ProblemCounter()

  lazy val backupDesc = universe.backupPartHandler().loadBackup()

  def verify(t: VerifyConf) = {
    startMeasuring()
    problemCounter = new ProblemCounter()
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
    l.info(s"Verification completed with ${problemCounter.current} errors in "+measuredTime())
    problemCounter.current
  }

  def verifyHashes() {
    val it = backupDesc.files.iterator
    while (it.hasNext) {
      it.next match {
        case f: FileDescription =>
          val chain = getHashlistForFile(f)
          val missing = chain.filterNot { universe.blockHandler().isPersisted }
          if (!missing.isEmpty) {
            problemCounter += missing.size
            l.warn(s"Missing ${missing.size} blocks for $f")
          }
        case _ =>
      }
    }
  }

  val random = new Random()

  def getRandomXElements[T](x: Int, array: Array[T]) = {
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
    val files = backupDesc.files.toArray
    var probes = ((percent * 1.0 / 100.0) * files.size).toInt + 1
    if (probes > files.size)
      probes = files.size
    if (probes <= 0)
      return
    val tests = getRandomXElements(probes, files)
    setMaximums(tests)
    tests.par.foreach { file =>
      val in = getInputStream(file)
      val hos = new HashingOutputStream(config.hashAlgorithm)
      copy(in, hos)
      if (!util.Arrays.equals(file.hash, hos.out.get)) {
        l.warn("File " + file + " is broken in backup")
        l.warn(Utils.encodeBase64Url(file.hash) + " " + Utils.encodeBase64Url(hos.out.get))
        problemCounter += 1
      }
      byteCounter += file.size
      fileCounter += 1
    }
  }

}
