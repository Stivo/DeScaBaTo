package ch.descabato

import java.io.File
import java.util.{ HashMap => JHashMap }
import java.nio.file.Files
import java.nio.file.SimpleFileVisitor
import scala.collection.mutable.Buffer
import scala.collection.mutable
import scala.collection.immutable.SortedMap
import Streams._
import java.io.FileInputStream
import java.security.MessageDigest
import java.util.Arrays
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.ByteArrayInputStream
import java.io.SequenceInputStream
import java.util.Enumeration
import com.fasterxml.jackson.annotation.JsonIgnore
import java.io.IOException
import java.io.PrintStream
import java.util.Date
import java.security.DigestOutputStream
import scala.io.Source
import scala.util.Random
import java.io.FileNotFoundException
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.NoSuchFileException
import net.java.truevfs.access.TConfig
import net.java.truevfs.kernel.spec.spi.FsDriverMapFactory
import net.java.truevfs.kernel.spec.sl.FsDriverMapLocator

/**
 * The configuration to use when working with a backup folder.
 */
case class BackupFolderConfiguration(folder: File, prefix: String = "", @JsonIgnore var passphrase: Option[String] = None, newBackup: Boolean = false) {
  def this() = this(null)
  var version = 1
  var serializerType = "smile"
  @JsonIgnore
  def serialization(typ: String = serializerType) = typ match {
    case "smile" => new SmileSerialization
    case "json" => new JsonSerialization
  }
  var keyLength = 128
  var compressor = CompressionMode.gzip
  def hashLength = getMessageDigest().getDigestLength()
  var hashAlgorithm = "MD5"
  @JsonIgnore def getMessageDigest() = MessageDigest.getInstance(hashAlgorithm)
  var blockSize: Size = Size("16Kb")
  var volumeSize: Size = Size("100Mb")
  var threads: Int = 1
  var checkPointEvery: Size = volumeSize
  val useDeltas = false
  var hasPassword = passphrase.isDefined
  var renameDetection = true
  var redundancyEnabled = false
  var metadataRedundancy: Int = 20
  var volumeRedundancy: Int = 5
  var saveSymlinks: Boolean = true
  @JsonIgnore lazy val fileManager = new FileManager(this)
  @JsonIgnore def raes = if (hasPassword) ".raes" else ""
}

object InitBackupFolderConfiguration {
  def apply(option: BackupFolderOption, passphrase: Option[String]) = {
    val out = BackupFolderConfiguration(option.backupDestination(), option.prefix(), passphrase)
    option match {
      case o: ChangeableBackupOptions =>
        o.keylength.foreach(out.keyLength = _)
        o.volumeSize.foreach(out.volumeSize = _)
        o.threads.foreach(out.threads = _)
        o.checkpointEvery.foreach(out.checkPointEvery = _)
        o.renameDetection.foreach(out.renameDetection = _)
        o.noRedundancy.foreach(b => out.redundancyEnabled = !b)
        o.volumeRedundancy.foreach(out.volumeRedundancy = _)
        o.metadataRedundancy.foreach(out.metadataRedundancy = _)
        o.dontSaveSymlinks.foreach(b => out.saveSymlinks = !b)
      case _ =>
    }
    option match {
      case o: CreateBackupOptions =>
        o.serializerType.foreach(out.serializerType = _)
        o.compression.foreach(x => out.compressor = x)
        o.blockSize.foreach(out.blockSize = _)
        o.hashAlgorithm.foreach(out.hashAlgorithm = _)
      case _ => // TODO
    }
    out
  }

  def merge(old: BackupFolderConfiguration, supplied: BackupFolderOption, passphrase: Option[String]) = {
    old.passphrase = passphrase
    var changed = false
    supplied match {
      case o: ChangeableBackupOptions =>
        o.keylength.foreach { changed = true; old.keyLength = _ }
        o.volumeSize.foreach { changed = true; old.volumeSize = _ }
        o.threads.foreach { changed = true; old.threads = _ }
        o.checkpointEvery.foreach { changed = true; old.checkPointEvery = _ }
        o.renameDetection.foreach { changed = true; old.renameDetection = _ }
        o.volumeRedundancy.foreach { changed = true; old.volumeRedundancy = _ }
        o.metadataRedundancy.foreach { changed = true; old.metadataRedundancy = _ }
        o.volumeRedundancy.foreach { changed = true; old.volumeRedundancy = _ }
        o.dontSaveSymlinks.foreach { x => changed = true; old.saveSymlinks = !x }
      // TODO other properties that can be set again
      case _ =>
    }
    l.debug("Configuration after merge " + old)
    (old, changed)
  }

}

object BackupVerification {
  trait VerificationResult

  case object PasswordNeeded extends VerificationResult

  case object BackupDoesntExist extends Exception("This backup was not found.\nSpecify backup folder and prefix if needed")
    with VerificationResult with BackupException

  case object OK extends VerificationResult
}

/**
 * Loads a configuration and verifies the command line arguments
 */
class BackupConfigurationHandler(supplied: BackupFolderOption) extends Utils {

  val mainFile = supplied.prefix() + "backup.json"
  val folder: File = supplied.backupDestination()
  def hasOld = new File(folder, mainFile).exists()
  def loadOld(): BackupFolderConfiguration = {
    val json = new JsonSerialization()
    json.readObject[BackupFolderConfiguration](new FileInputStream(new File(folder, mainFile))).left.get
  }

  def write(out: BackupFolderConfiguration) {
    val json = new JsonSerialization()
    val fos = new UnclosedFileOutputStream(new File(folder, mainFile))
    json.writeObject(out, fos)
    // writeObject closes
  }

  def verify(existing: Boolean): BackupVerification.VerificationResult = {
    import BackupVerification._
    if (existing && !hasOld) {
      return BackupDoesntExist
    }
    if (hasOld) {
      if (loadOld().hasPassword && supplied.passphrase.isEmpty) {
        return PasswordNeeded
      }
    }
    OK
  }

  def configure(passphrase: Option[String]): BackupFolderConfiguration = {
    def initTrueVfs(conf: BackupFolderConfiguration) {
      for (p <- conf.passphrase) {
        TConfig.current().setArchiveDetector(TrueVfs.newArchiveDetector1(FsDriverMapLocator.SINGLETON, ".zip.raes", p.toCharArray(), conf.keyLength))
      }
    }
    if (hasOld) {
      val oldConfig = loadOld()
      val (out, changed) = InitBackupFolderConfiguration.merge(oldConfig, supplied, passphrase)
      if (changed) {
        write(out)
      }
      initTrueVfs(out)
      out
    } else {
      folder.mkdirs()
      val out = InitBackupFolderConfiguration(supplied, passphrase)
      write(out)
      initTrueVfs(out)
      out
    }
  }

}

object BackupUtils {
  def findOld[T <: BackupPart](file: File, oldMap: mutable.Map[String, BackupPart])(implicit manifest: Manifest[T]): (Option[T]) = {
    val path = file.getCanonicalPath
    // if the file is in the map, no other file can have the same name. Therefore we remove it.
    val out = oldMap.remove(path)
    if (out.isDefined &&
      // file size has not changed, if it is a file
      (!(out.get.isInstanceOf[FileDescription]) || out.get.size == file.length()) &&
      // if the backup part is of the wrong type => return (None, fa)
      manifest.runtimeClass.isAssignableFrom(out.get.getClass()) &&
      // if the file has attributes and the last modified date is different, return (None, fa)
      (out.get.attrs != null && !out.get.attrs.hasBeenModified(file))) {
      // backup part is correct and unchanged
      Some(out.get.asInstanceOf[T])
    } else {
      None
    }
  }
}

trait BackupProgressReporting extends Utils {
  lazy val scanCounter = new StandardCounter("Files found: ")
  lazy val fileCounter = new StandardMaxValueCounter("Files", 0) {}
  lazy val byteCounter = new StandardMaxValueCounter("Data", 0) with ETACounter {
    override def format = s"${readableFileSize(current)}/${readableFileSize(maxValue)} ${percent}%"
  }

  def setMaximums(files: Iterable[BackupPart]) {
    fileCounter.maxValue = files.size
    byteCounter.maxValue = files.map(_.size).sum
    fileCounter.current = 0
    byteCounter.current = 0
  }

  def updateProgress() {
    ProgressReporters.updateWithCounters(fileCounter :: byteCounter :: Nil)
  }
}

abstract class BackupIndexHandler extends Utils {
  val config: BackupFolderConfiguration

  lazy val fileManager = config.fileManager

  def loadOldIndex(temp: Boolean = false, date: Option[Date] = None): Buffer[BackupPart] = {
    val (filesToLoad, hasDeltas) = if (date.isDefined) {
      (fileManager.getBackupForDate(date.get).toArray, false)
    } else {
      fileManager.getBackupAndUpdates(temp)
    }
    val updates = filesToLoad.flatMap(x => fileManager.filesDelta.read(x)).fold(Buffer[UpdatePart]())(_ ++ _).seq
    if (hasDeltas) {
      val map = mutable.LinkedHashMap[String, BackupPart]()
      updates.foreach {
        case FileDeleted(path) => map -= path
        case x: BackupPart => map(x.path) = x
      }
      map.values.toBuffer
    } else {
      updates.asInstanceOf[Buffer[BackupPart]]
    }
  }

  def loadOldIndexAsMap(temp: Boolean = false): mutable.Map[String, BackupPart] = {
    val buffer = loadOldIndex(temp)
    val oldBackupFilesRemaining = mutable.HashMap[String, BackupPart]()
    buffer.foreach(x => oldBackupFilesRemaining += x.path -> x)
    oldBackupFilesRemaining
  }

  def partitionFolders(x: Seq[BackupPart]) = {
    val folders = Buffer[FolderDescription]()
    val files = Buffer[FileDescription]()
    val links = Buffer[SymbolicLink]()
    x.foreach {
      case x: FileDescription => files += x
      case x: FolderDescription => folders += x
      case x: SymbolicLink => links += x
    }
    (folders, files, links)
  }

}

abstract class BackupDataHandler extends BackupIndexHandler {
  self: BlockStrategy =>
  import BAWrapper2.byteArrayToWrapper
  type HashChainMap = mutable.HashMap[BAWrapper2, Array[Byte]]

  lazy val delta = config.useDeltas && !loadOldIndex().isEmpty

  def oldBackupHashChains: mutable.Map[BAWrapper2, Array[Byte]] = {
    def loadFor(x: Iterable[File]) = {
      x.flatMap(x => fileManager.hashchains.read(x)).fold(Buffer())(_ ++ _).toBuffer
    }
    val temps = loadFor(fileManager.hashchains.getTempFiles())
    if (!temps.isEmpty) {
      fileManager.hashchains.write(temps)
      fileManager.hashchains.deleteTempFiles()
    }
    val list = loadFor(fileManager.hashchains.getFiles())
    val map = new HashChainMap
    map ++= list
    map
  }

  // TODO this is not needed for backups. Only for restores. For backups, only the keys are needed.
  var hashChainMap: HashChainMap = new HashChainMap()

  def importOldHashChains() {
    hashChainMap ++= oldBackupHashChains
  }

  def hashChainSeq(a: Array[Byte]) = a.grouped(config.getMessageDigest.getDigestLength()).toSeq

  def getHashChain(fd: FileDescription): Seq[Array[Byte]] = {
    if (fd.size <= config.blockSize.bytes) {
      List(fd.hash)
    } else {
      hashChainSeq(hashChainMap.get(fd.hash).get)
    }
  }

  def statistics(x: Seq[BackupPart]) = {
    val num = x.size
    val totalSize = new Size(x.map(_.size).sum)
    var out = f"$num%8d, $totalSize"
    if ((1 to 10) contains num) {
      out += "\n" + x.map(_.path).mkString("\n")
    }
    out
  }

}

class BackupHandler(val config: BackupFolderConfiguration)
  extends BackupDataHandler with BackupProgressReporting {
  self: BlockStrategy =>

  // Needed for backing up
  var hashChainMapNew: HashChainMap = new HashChainMap()
  var hashChainMapCheckpoint: HashChainMap = new HashChainMap()

  def checkpoint(seq: Seq[FileDescription]) = {
    val (toSave, toKeep) = seq.partition(x => blockExists(getHashChain(x).last))
    fileManager.files.write(toSave.toBuffer, true)
    if (!hashChainMapCheckpoint.isEmpty) {
      val (toSave, toKeep) = hashChainMapCheckpoint.toBuffer.partition(x => blockExists(hashChainSeq(x._2).last))
      fileManager.hashchains.write(toSave.toBuffer, true)
      hashChainMapCheckpoint.clear
      hashChainMapCheckpoint ++= toKeep
    }
    toKeep
  }

  var failed = Buffer[FileDescription]()

  var deletedCopy: Buffer[FileDescription] = Buffer()

  def backup(files: Seq[File]) {
    config.folder.mkdirs()
    l.info("Starting backup")
    // Walk tree and compile to do list
    val visitor = new OldIndexVisitor(loadOldIndexAsMap(true), recordNew = true, recordUnchanged = true, progress = Some(scanCounter)).walk(files)
    val (newParts, unchanged, deleted) = (visitor.newFiles, visitor.unchangedFiles, visitor.deleted)
    l.info("New Files      : " + statistics(newParts))
    l.info("Unchanged files: " + statistics(unchanged))
    l.info("Deleted files  : " + statistics(deleted))
    if ((newParts ++ deleted).isEmpty) {
      l.info("No files have been changed, aborting backup")
      return
    }
    val fileListOut = Buffer[BackupPart]()
    val (newFolders, newFiles, links) = partitionFolders(newParts)
    if (config.renameDetection) {
      deletedCopy = partitionFolders(deleted)._2
    }
    // Backup files 

    setMaximums(newFiles)
    importOldHashChains
    var list = newFiles.toList
    var toSave: Seq[FileDescription] = Buffer.empty
    while (!list.isEmpty) {
      var sum = config.checkPointEvery.bytes
      val cur = list.takeWhile { x => val out = sum > 0; sum -= x.size; out }
      val temp = cur.flatMap(backupFileDesc(_))
      fileListOut ++= temp
      toSave ++= temp
      toSave = checkpoint(toSave)
      l.debug(s"Checkpointing: ${temp.size} files processed, ${toSave.size} pending to be saved")
      list = list.drop(cur.size)
    }

    // Clean up, handle failed entries

    if (!failed.isEmpty) {
      l.info(failed.size + " files could not be backed up due to file locks, trying again.")
      val copy = failed.toBuffer
      failed.clear()
      val succeeded = copy.flatMap(fileDesc => backupFileDesc(fileDesc))
      if (succeeded.size == copy.size) {
        l.info("All files backed up now")
      } else {
        l.info(copy.size - succeeded.size + " Files could not be backed up. See log for a complete list")
        failed.foreach { f => l.debug("File " + f + " was not backed up due to locks") }
      }
    }

    // finish writing, complete the backup

    finishWriting
    if (!hashChainMapNew.isEmpty) {
      fileManager.hashchains.write(hashChainMapNew.toBuffer)
      fileManager.hashchains.deleteTempFiles()
    }
    if (!delta) {
      fileListOut ++= unchanged
    }
    fileListOut ++= newFolders
    if (config.saveSymlinks)
      fileListOut ++= links
    if (delta) {
      val toWrite = deleted.map { case x: BackupPart => FileDeleted(x.path) }.toBuffer ++ fileListOut
      fileManager.filesDelta.write(toWrite)
    } else {
      fileManager.files.write(fileListOut)
      fileManager.files.deleteTempFiles()
    }
    l.info("Backup completed")
  }

  val useNoCompressionList = true

  lazy val noCompressionSet: Set[String] = {
    try {
      val source = Source.fromInputStream(classOf[BackupHandler].getResourceAsStream("/default_compressed_extensions.txt"))
      val out = source.getLines.toList.map(_.takeWhile(_ != '#').trim.toLowerCase()).filterNot(_.isEmpty()).toSet
      l.debug("Not going to compress these file types: ")
      l.debug(out.toString)
      out
    } catch {
      case io @ (_: IOException | _: NullPointerException) =>
        l.info("No compression list not found " + io.getMessage())
        Set()
    }
  }

  /**
   * Returns true if the compression should be disabled.
   */
  def compressionFor(fileDesc: FileDescription) = {
    val index = fileDesc.path.lastIndexOf(".")
    noCompressionSet contains (fileDesc.path.drop(index).toLowerCase())
  }

  def checkForRename(file: File, fileDesc: FileDescription, input: FileInputStream): Option[FileDescription] = {
    try {
      lazy val firstBlockHash = {
        val buf = Array.ofDim[Byte](config.blockSize.bytes.toInt)
        val read = input.read(buf)
        val digest = config.getMessageDigest
        digest.update(buf, 0, read)
        digest.digest()
      }
      def checkFilter(candidates: Seq[FileDescription], fil: (String, FileDescription => Boolean)) = {
        if (candidates.isEmpty) {
          throw new IllegalStateException()
        }
        val out = candidates.filter(fil._2)
        //        l.info(s"${fil._1}: ${out.size}")
        out
      }
      val filters = List(
        ("Same size", { fd: FileDescription => fd.size == fileDesc.size }),
        ("same modification date", { fd: FileDescription => !fd.attrs.hasBeenModified(file) }),
        ("same name", { fd: FileDescription => println(fd.pathParts.mkString(" / ")); fd.pathParts.last == file.getName() }),
        ("same first block", { fd: FileDescription => Arrays.equals(getHashChain(fd).head, firstBlockHash) }))
      filters.foldLeft(deletedCopy.toSeq)((x, y) => checkFilter(x, y)).headOption
    } catch {
      case i: IllegalStateException => None
    }
  }

  def backupFileDesc(fileDesc: FileDescription): Option[FileDescription] = {
    val byteCounterbackup = byteCounter.current

    var fis: FileInputStream = null
    try {
      // This is a new file, so we start hashing its contents, fill those in and return the same instance
      val file = new File(fileDesc.path)
      fis = new FileInputStream(file)
      if (config.renameDetection) {
        val oldOne = checkForRename(file, fileDesc, fis)
        for (old <- oldOne) {
          l.info("Detected rename from " + old.path + " to " + fileDesc.path)
          deletedCopy -= old
          val out = old.copy(path = fileDesc.path)
          out.hash = old.hash
          return Some(out)
        }
        fis.getChannel.position(0)
      }
      lazy val compressionDisabled = compressionFor(fileDesc)
      val (fileHash, hashList) = ObjectPools.baosPool.withObject((), {
        out: ByteArrayOutputStream =>
          val md = config.getMessageDigest
          def hashAndWriteBlock(buf: Array[Byte]) {
            val hash = md.digest(buf)
            out.write(hash)
            if (!blockExists(hash)) {
              writeBlock(hash, buf, compressionDisabled)
            }
            byteCounter += buf.size
            updateProgress
          }
          val hash = {
            val blockHasher = new BlockOutputStream(config.blockSize.bytes.toInt, hashAndWriteBlock _)
            val hos = new HashingOutputStream(config.hashAlgorithm)
            val sis = new SplitInputStream(fis, blockHasher :: hos :: Nil)
            sis.readComplete
            sis.close()
            hos.out.get
          }
          (hash, out.toByteArray())
      })
      assert(hashList.length != 0)
      if (hashList.length == fileHash.length) {
        null
      } else {
        // use same instance for the keys
        val key: BAWrapper2 = fileHash
        if (!hashChainMap.contains(key)) {
          hashChainMap += ((key, hashList))
          hashChainMapNew += ((key, hashList))
          hashChainMapCheckpoint += ((key, hashList))
        }
      }
      fileDesc.hash = fileHash
      fileCounter += 1
      updateProgress()
      Some(fileDesc)
    } catch {
      case io: IOException if (io.getMessage().contains("The process cannot access the file")) =>
        failed += fileDesc
        l.warn("Could not backup file " + fileDesc.path + " because it is locked.")
        None
      // TODO linux add case for symlinks
      case io: IOException if (io.getMessage().contains("The system cannot find the")) =>
        l.info(s"File ${fileDesc.path} has been deleted since starting the backup")
        None
      case io: FileNotFoundException if (io.getMessage().contains("Access is denied")) =>
        l.info(s"File ${fileDesc.path} can not be accessed")
        failed += fileDesc
        None
      case e: IOException => throw e
    } finally {
      if (fis != null)
        fis.close()
    }
  }

}

abstract class ReadingHandler(option: Option[Date] = None) extends BackupDataHandler {
  self: BlockStrategy =>

  def getInputStream(fd: FileDescription): InputStream = {
    val hashes = getHashChain(fd)
    val enumeration = new Enumeration[InputStream]() {
      val hashIterator = hashes.iterator
      def hasMoreElements = hashIterator.hasNext
      def nextElement = readBlock(hashIterator.next, true)
    }
    new SequenceInputStream(enumeration)
  }

}

class RestoreHandler(val config: BackupFolderConfiguration)
  extends ReadingHandler() with BackupProgressReporting {
  self: BlockStrategy =>

  def restore(options: RestoreConf, d: Option[Date] = None) {
    implicit val o = options
    importOldHashChains
    val filesInBackup = loadOldIndex(date = d)
    val filtered = if (o.pattern.isDefined) filesInBackup.filter(x => x.path.contains(options.pattern())) else filesInBackup
    l.info("Going to restore " + statistics(filtered))
    setMaximums(filtered)
    val (folders, files, links) = partitionFolders(filtered)
    folders.foreach(restoreFolderDesc(_))
    links.foreach(restoreLink)
    files.foreach(restoreFileDesc)
    folders.foreach(restoreFolderDesc(_, false))
    finishReading()
  }

  def restoreFromDate(t: RestoreConf, d: Date) {
    restore(t, Some(d))
  }

  def makePath(fd: BackupPart)(implicit options: RestoreConf) : File = {
    if (options.restoreToOriginalPath()) {
      return new File(fd.path)
    }
    val dest = new File(options.restoreToFolder())
    def cleaned(s: String) = if (Utils.isWindows) s.replaceAllLiterally(":", "_") else s
    if (options.relativeToFolder.isDefined) {
      new File(dest, fd.relativeTo(new File(options.relativeToFolder())))
    } else {
      new File(dest, cleaned(fd.path))
    }

  }

  def restoreLink(link: SymbolicLink)(implicit options: RestoreConf) {
    try {
      val path = makePath(link)
      // TODO if link links into backup, path should be updated
      Files.createLink(path.toPath(), new File(link.linkTarget).toPath())
      link.applyAttrsTo(path)
    } catch {
      case e @ (_: UnsupportedOperationException | _: NoSuchFileException) =>
        l.warn("Symbolic link to " + link.linkTarget + " can not be restored")
        logException(e)
    }
  }

  def restoreFolderDesc(fd: FolderDescription, count: Boolean = true)(implicit options: RestoreConf) {
    val restoredFile = makePath(fd)
    restoredFile.mkdirs()
    fd.applyAttrsTo(restoredFile)
    if (count)
      fileCounter += 1
    updateProgress
  }

  def restoreFileDesc(fd: FileDescription)(implicit options: RestoreConf) {
    var closeAbles = Buffer[{ def close(): Unit }]()
    try {
      val restoredFile = makePath(fd)
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
      val fos = new UnclosedFileOutputStream(restoredFile)
      closeAbles += fos
      val dos = new DigestOutputStream(fos, config.getMessageDigest)
      closeAbles += dos
      val in = getInputStream(fd)
      closeAbles += in
      copy(in, dos)
      val hash = dos.getMessageDigest().digest()
      if (!Arrays.equals(fd.hash, hash)) {
        l.warn("Error while restoring file, hash is not correct")
      }
      if (restoredFile.length() != fd.size) {
        l.warn(s"Restoring failed for $restoredFile (old size: ${fd.size}, now: ${restoredFile.length()}")
      }
      fd.applyAttrsTo(restoredFile)
    } catch {
      case e @ BackupCorruptedException(f, false) =>
        finishReading
        throw e
      case e: Exception =>
        finishReading
        l.warn("Exception while restoring " + fd.path + " (" + e.getMessage() + ")")
        logException(e)
    } finally {
      closeAbles.foreach(_.close)
    }
    fileCounter += 1
    byteCounter += fd.size
    updateProgress
  }

}

class ProblemCounter extends Counter {
  def name = "Problems"
}

class VerifyHandler(val config: BackupFolderConfiguration)
  extends ReadingHandler with Utils with BackupProgressReporting {
  self: BlockStrategy =>
  val block: BlockStrategy = self

  var problemCounter = new ProblemCounter()

  def verify(t: VerifyConf) = {
    problemCounter = new ProblemCounter()
    try {
      block.verify(problemCounter)
      importOldHashChains
      verifyHashes()
      verifySomeFiles(t.percentOfFilesToCheck())
    } catch {
      case x: Error =>
        problemCounter += 1
        l.warn("Aborting verification because of error ", x)
        logException(x)
    }
    problemCounter.current
  }

  val index = loadOldIndex(false)

  def verifyHashes() {
    val it = index.iterator
    while (it.hasNext) {
      it.next match {
        case f: FileDescription =>
          val chain = getHashChain(f)
          val missing = chain.filterNot { blockExists }
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
      swap(i, random.nextInt(array.length - 1 - i) + i)
    }
    array.take(end)
  }

  def verifySomeFiles(percent: Int) {

    var files = partitionFolders(index)._2.toArray
    val probes = ((percent * 1.0 / 100.0) * files.size).toInt + 1
    val tests = getRandomXElements(probes, files)
    setMaximums(tests)
    tests.foreach { file =>
      val in = getInputStream(file)
      val hos = new HashingOutputStream(config.hashAlgorithm)
      copy(in, hos)
      if (!Arrays.equals(file.hash, hos.out.get)) {
        l.warn("File " + file + " is broken in backup")
        l.warn(Utils.encodeBase64Url(file.hash) + " " + Utils.encodeBase64Url(hos.out.get))
        problemCounter += 1
      }
      byteCounter += file.size
      fileCounter += 1
      updateProgress
    }
  }

}
