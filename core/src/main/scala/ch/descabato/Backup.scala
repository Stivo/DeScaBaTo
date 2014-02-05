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
import net.java.truevfs.kernel.spec.FsAccessOption

/**
 * The configuration to use when working with a backup folder.
 */
case class BackupFolderConfiguration(folder: File, prefix: String = "", @JsonIgnore var passphrase: Option[String] = None, newBackup: Boolean = false) {
  def this() = this(null)
  var version = ch.descabato.version.BuildInfo.version
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
        o.compression.foreach(x => out.compressor = x)
      case _ =>
    }
    option match {
      case o: CreateBackupOptions =>
        o.serializerType.foreach(out.serializerType = _)
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
        if (o.keylength.isSupplied) {
          o.keylength.foreach(old.keyLength = _)
          changed = true
        }
        if (o.volumeSize.isSupplied) {
          o.volumeSize.foreach(old.volumeSize = _)
          changed = true
        }
        if (o.threads.isSupplied) {
          o.threads.foreach(old.threads = _)
          changed = true
        }
        if (o.checkpointEvery.isSupplied) {
          o.checkpointEvery.foreach(old.checkPointEvery = _)
          changed = true
        }
        if (o.renameDetection.isSupplied) {
          o.renameDetection.foreach(old.renameDetection = _)
          changed = true
        }
        if (o.noRedundancy.isSupplied) {
          o.noRedundancy.foreach(b => old.redundancyEnabled = !b)
          changed = true
        }
        if (o.volumeRedundancy.isSupplied) {
          o.volumeRedundancy.foreach(old.volumeRedundancy = _)
          changed = true
        }
        if (o.metadataRedundancy.isSupplied) {
          o.metadataRedundancy.foreach(old.metadataRedundancy = _)
          changed = true
        }
        if (o.dontSaveSymlinks.isSupplied) {
          o.dontSaveSymlinks.foreach(b => old.saveSymlinks = !b)
          changed = true
        }
        if (o.compression.isSupplied) {
          o.compression.foreach(x => old.compressor = x)
          changed = true
        }
      // TODO other properties that can be set again
      // TODO generate this code omg
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
  def hasOld = new File(folder, mainFile).exists() && loadOld().isDefined
  def loadOld(): Option[BackupFolderConfiguration] = {
    val json = new JsonSerialization()
    // TODO a backup.json that is invalid is a serious problem. Should throw exception
    json.readObject[BackupFolderConfiguration](new FileInputStream(new File(folder, mainFile))) match {
      case Left(x) => Some(x)
      case _ => None
    }
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
      if (loadOld().get.hasPassword && supplied.passphrase.isEmpty) {
        return PasswordNeeded
      }
    }
    OK
  }

  def verifyAndInitializeSetup(conf: BackupFolderConfiguration) {
    if (conf.redundancyEnabled && CommandLineToolSearcher.lookUpName("par2").isEmpty) {
      throw ExceptionFactory.newPar2Missing
    }
    def initTrueVfs(conf: BackupFolderConfiguration) {
      for (p <- conf.passphrase) {
        TConfig.current().setArchiveDetector(TrueVfs.newArchiveDetector1(FsDriverMapLocator.SINGLETON, ".zip.raes", p.toCharArray(), conf.keyLength))
      }
      TConfig.current().setAccessPreference(FsAccessOption.STORE, true)
      // Disabled to distinguish files being changed and completed files
      //TConfig.current().setAccessPreference(FsAccessOption.GROW, true)
    }
    initTrueVfs(conf)
  }

  def configure(passphrase: Option[String]): BackupFolderConfiguration = {
    if (hasOld) {
      val oldConfig = loadOld().get
      val (out, changed) = InitBackupFolderConfiguration.merge(oldConfig, supplied, passphrase)
      if (changed) {
        write(out)
      }
      verifyAndInitializeSetup(out)
      out
    } else {
      folder.mkdirs()
      val out = InitBackupFolderConfiguration(supplied, passphrase)
      write(out)
      verifyAndInitializeSetup(out)
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
    // TODO this should be rewritten in a sensitive way
    val (filesToLoad, hasDeltas) = if (date.isDefined) {
      (fileManager.getBackupForDate(date.get).toArray, false)
    } else {
      fileManager.getBackupAndUpdates(false)
    }
    val tempFiles = if (temp) {
      (fileManager.getBackupAndUpdates(true)._1.toBuffer -- filesToLoad).toSeq
    } else {
      Nil
    }
    val updates = filesToLoad.flatMap(x => fileManager.filesDelta.read(x, OnFailureTryRepair)).fold(Buffer[UpdatePart]())(_ ++ _).seq
    val withTemp = if (temp) {
      (fileManager.getBackupAndUpdates(true)._1.toBuffer -- filesToLoad).toSeq
      updates ++ filesToLoad.flatMap(x => fileManager.filesDelta.read(x, OnFailureDelete)).fold(Buffer[UpdatePart]())(_ ++ _).seq
    } else {
      updates
    }
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
    var candidates = Buffer[BackupPart]()
    folders.view.filter(_.isFolder).foreach { folder =>
      if (folder.path == "/") return List(folder.asInstanceOf[FolderDescription])
      // compare with each candidate
      var foundACandidate = false
      candidates = candidates.map { candidate =>
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
    candidates.flatMap { case x: FolderDescription => List(x) }.toList
  }

}

abstract class BackupDataHandler extends BackupIndexHandler {
  self: BlockStrategy =>
  import BAWrapper2.byteArrayToWrapper
  type HashListMap = mutable.HashMap[BAWrapper2, Array[Byte]]

  lazy val delta = config.useDeltas && !loadOldIndex().isEmpty

  def oldBackupHashLists: mutable.Map[BAWrapper2, Array[Byte]] = {
    def loadFor(x: Iterable[File], failureOption: ReadFailureOption) = {
      x.flatMap(x => fileManager.hashlists.read(x, failureOption)).fold(Buffer())(_ ++ _).toBuffer
    }
    val temps = loadFor(fileManager.hashlists.getTempFiles(), OnFailureDelete)
    if (!temps.isEmpty) {
      fileManager.hashlists.write(temps)
      fileManager.hashlists.deleteTempFiles()
    }
    val list = loadFor(fileManager.hashlists.getFiles(), OnFailureTryRepair)
    val map = new HashListMap
    map ++= list
    map
  }

  // TODO this is not needed for backups. Only for restores. For backups, only the keys are needed.
  var hashListMap: HashListMap = new HashListMap()

  def importOldHashLists() {
    hashListMap ++= oldBackupHashLists
  }

  def hashListSeq(a: Array[Byte]) = a.grouped(config.getMessageDigest.getDigestLength()).toSeq

  def getHashList(fd: FileDescription): Seq[Array[Byte]] = {
    if (fd.size <= config.blockSize.bytes) {
      List(fd.hash)
    } else {
      hashListSeq(hashListMap.get(fd.hash).get)
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
  var hashListMapNew: HashListMap = new HashListMap()
  var hashListMapCheckpoint: HashListMap = new HashListMap()

  def checkpoint(seq: Seq[FileDescription]) = {
    val (saveHashlists, keepHashlists) = hashListMapCheckpoint.toBuffer.partition(x => hashListSeq(x._2).forall(blockExists))
    // checkpoint the hashlists
    if (!hashListMapCheckpoint.isEmpty) {
      if (!saveHashlists.isEmpty)
        fileManager.hashlists.write(saveHashlists.toBuffer, true)
      hashListMapCheckpoint.clear
      hashListMapCheckpoint ++= keepHashlists
    }
    // checkpoint the files, after the corresponding hash lists have been saved
    val (toSave, toKeep) = seq.partition{
      x => 
        val list = getHashList(x)
        if (list.size == 1) blockExists(list.head)
        else saveHashlists.map(_._1).contains(x.hash: BAWrapper2)
    }
    if (!toSave.isEmpty)
      fileManager.files.write(toSave.toBuffer, true)
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
    importOldHashLists
    l.info("Counting of files done")
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
    var list = newFiles.toList
    var toSave: Seq[FileDescription] = Buffer.empty
    while (!list.isEmpty) {
      var sum = config.checkPointEvery.bytes
      // This ensures that at least one element is taken.
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
    if (!hashListMapNew.isEmpty) {
      fileManager.hashlists.write(hashListMapNew.toBuffer)
      fileManager.hashlists.deleteTempFiles()
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
        ("same first block", { fd: FileDescription => Arrays.equals(getHashList(fd).head, firstBlockHash) }))
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
        if (!hashListMap.contains(key)) {
          hashListMap += ((key, hashList))
          hashListMapNew += ((key, hashList))
          hashListMapCheckpoint += ((key, hashList))
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
    val hashes = getHashList(fd)
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

  var roots: Iterable[FolderDescription] = Buffer.empty
  var relativeToRoot: Boolean = false

  def getRoot(sub: String) = {
    // TODO this asks for a refactoring
    val fd = new FolderDescription(sub, null)
    roots.find { x => var r = isRelated(x, fd); r == IsSubFolder || r == IsSame }
  }

  def initRoots(folders: Seq[BackupPart]) {
    roots = detectRoots(folders)
    relativeToRoot = roots.size == 1 || roots.map(_.name).toList.distinct.size == roots.size
  }

  def restore(options: RestoreConf, d: Option[Date] = None) {
    implicit val o = options
    importOldHashLists
    val filesInBackup = loadOldIndex(date = d)
    initRoots(filesInBackup)
    val filtered = if (o.pattern.isDefined) filesInBackup.filter(x => x.path.contains(options.pattern())) else filesInBackup
    l.info("Going to restore " + statistics(filtered))
    setMaximums(filtered)
    val (folders, files, links) = partitionFolders(filtered)
    folders.foreach(restoreFolderDesc(_))
    files.foreach(restoreFileDesc)
    links.foreach(restoreLink)
    folders.foreach(restoreFolderDesc(_, false))
    finishReading()
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
      case e @ (_: UnsupportedOperationException | _: NoSuchFileException) =>
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
    updateProgress
  }

  def restoreFileDesc(fd: FileDescription)(implicit options: RestoreConf) {
    var closeAbles = Buffer[{ def close(): Unit }]()
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
      importOldHashLists
      verifyHashes()
      verifySomeFiles(t.percentOfFilesToCheck())
      finishReading
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
          val chain = getHashList(f)
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
      val chooseFrom = array.length - 1 - i
      if (chooseFrom > 0)
    	swap(i, random.nextInt(chooseFrom) + i)
    }
    array.take(end)
  }

  def verifySomeFiles(percent: Int) {
    var files = partitionFolders(index)._2.toArray
    var probes = ((percent * 1.0 / 100.0) * files.size).toInt + 1
    if (probes > files.size) 
      probes = files.size
    if (probes <= 0)
      return
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
