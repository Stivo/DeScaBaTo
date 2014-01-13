package backup

import java.io.FileInputStream
import java.io.FileOutputStream
import scala.collection.mutable.Buffer
import java.io.File
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.util.Comparator
import java.util.Arrays
import scala.collection.mutable.HashMap
import java.io.BufferedInputStream
import com.sun.jna.platform.FileUtils
import scala.reflect.ManifestFactory
import scala.collection.mutable.ArrayBuffer
import backup.Streams.ObjectPools
import akka.actor.PoisonPill
import scala.concurrent.Await
import akka.pattern.{ ask, pipe }
import scala.collection.mutable
import java.io.InputStream
import java.io.ByteArrayInputStream

class BackupBaseHandler[T <: BackupFolderOption](val folder: T) extends DelegateSerialization with Utils {
  import Streams._
  import Utils._
  import BAWrapper2.byteArrayToWrapper
  
  type HashChainMap = HashMap[BAWrapper2, Array[Byte]]
  
  val blockStrategy = folder.getBlockStrategy
  
  lazy val oldBackupFiles : Iterable[BackupPart] = {
    implicit val options = folder
    val (filesToLoad, hasDeltas) = options.fileManager.getBackupAndUpdates
    val updates = filesToLoad.par.map(readObject[Buffer[UpdatePart]]).fold(Buffer[UpdatePart]())(_ ++ _).seq
    if (hasDeltas) {
      val map = mutable.LinkedHashMap[String, BackupPart]()
      updates.foreach{
        case FileDeleted(path, isFolder) => map -= path
        case x : BackupPart => map(x.path) = x
      }
      map.values.toBuffer
    } else {
      updates.asInstanceOf[Buffer[BackupPart]]
    }
  }

  lazy val oldBackupHashChains = {
    val filesToLoad = folder.fileManager.hashchains.getFiles()
    implicit val options = folder
    val list = filesToLoad.par.map(readObject[Buffer[(BAWrapper2, Array[Byte])]]).fold(Buffer())(_ ++ _).seq
    val map = new HashChainMap
    map ++= list
    map
  }
  var hashChainMap: HashChainMap = new HashChainMap()

  def importOldHashChains {
    hashChainMap ++= oldBackupHashChains
  }
  
  def getHashChain(fd: FileDescription) = {
    if (fd.hashChain == null) {
      fd.hash
    } else {
      hashChainMap.get(fd.hashChain).get
    }
  }
    
  lazy val backupProperties = new File(folder.backupFolder, "backup.properties")
    
  def loadBackupProperties(file: File = backupProperties) {
    val backup = folder.propertyFile
    val folderBefore = folder.backupFolder.getAbsolutePath()
    try {
      folder.propertyFile = file
      folder.propertyFileOverrides = true
      folder.readArgs(Map[String, String]())
      if (folder.backupFolder.getPath != folderBefore) {
        // the location of this backup has changed, the properties file needs to be updated
        folder.backupFolder = new File(folderBefore)
        folder.saveConfigFile(file)
      }
      serialization = folder.serialization
    } finally {
      folder.propertyFile = backup
      folder.propertyFileOverrides = false
    }
  }
    
}

class BackupHandler(val options: BackupOptions) extends BackupBaseHandler[BackupOptions](options){
  import Streams._
  import Utils._
  import scala.concurrent.duration._
  
  var changed: Boolean = false
  
  var hashChainMapTemp : HashChainMap = new HashChainMap()
  
  val oldBackupFilesRemaining = HashMap[String, BackupPart]()
  val deltaSet = Buffer[UpdatePart]()
  lazy val delta = options.useDeltas && !oldBackupFiles.isEmpty
  
  def backupFolder() {
    changed = false
    options.backupFolder.mkdirs()
    l.info("Starting to backup")
    val backupPropertyFile = backupProperties
    if (backupPropertyFile.exists) {
      l.info("Loading old backup properties")
      loadBackupProperties(backupPropertyFile)
    } else {
      serialization = folder.serialization
      l.info("Saving backup properties")
      options.saveConfigFile(backupPropertyFile)
    }
    val future = options.configureRemoteHandler
  	l.info(s"Found ${oldBackupFiles.size} previously backed up files")
  	oldBackupFilesRemaining ++= oldBackupFiles.map(x => (x.path, x))
  	importOldHashChains
  	Await.ready(future, 10 seconds)
  	Actors.remoteManager ! UploadFile(backupPropertyFile)
  	options.fileManager.volumes.getFiles().foreach { f =>
      Actors.remoteManager ! UploadFile(f, true)
      //TODO find better solution
    }
    var (counter, fileCounter, sizeCounter) = (0, 0, 0L)
    val files = Buffer[BackupPart]()
    var filesWritten = Buffer[File]()
    def newFiles() {
      
      if (delta) {
        if (!deltaSet.isEmpty) {
          val fi = options.fileManager.filesDelta.write(deltaSet)
          // if there was a delta added, changed must be true. We can upload directly.
          Actors.remoteManager ! UploadFile(fi)
        }
      } else {
        if (!files.isEmpty)
        	filesWritten += options.fileManager.files.write(files)
      }
      
      if (!hashChainMapTemp.isEmpty) {
          val fi = options.fileManager.hashchains.write(hashChainMapTemp.toBuffer)
	      Actors.remoteManager ! UploadFile(fi)
      }
      files.clear
      deltaSet.clear
      hashChainMapTemp = new HashChainMap()
    }
    def walk(file: File) {
      fileCounter += 1
      sizeCounter += (if (file.isFile()) file.length() else 0L)
      printDeleted(s"File #$fileCounter, total size: ${readableFileSize(sizeCounter)}), Analyzing ${file.getName}")
      if (files.length >= options.saveIndexEveryNFiles) {
        newFiles()
      }
      file.isDirectory() match {
        case true => files += backupFolderDesc(file); try {
          file.listFiles().foreach(walk)
        } catch {
          case e: Exception => {
        	  // TODO more fine grained handling of exceptions.
            //e.printStackTrace()
            l.error("Could not get children of "+file+", because of "+e)
          }
        }
        case false => files += backupFile(file)
      }
    }
    options.folderToBackup.foreach(walk)
    newFiles()
    blockStrategy.finishWriting
    l.info(s"Backup completed of $fileCounter (${readableFileSize(sizeCounter)})")
    if (!oldBackupFilesRemaining.isEmpty) {
      l.info(oldBackupFilesRemaining.size +" files have been deleted since last backup")
      changed = true
      if (delta) {
        options.fileManager.filesDelta.write(oldBackupFilesRemaining.values.map(x => FileDeleted(x)).toBuffer)
      }
    } 
    if (!changed) {
      l.info(s"Nothing has changed, removing this backup")
      filesWritten.foreach(_.delete)
    } else {
      filesWritten.foreach(x => Actors.remoteManager ! UploadFile(x))
    }
    if (options.redundancy.enabled) {
      val rh = new RedundancyHandler(options, options.redundancy)
      rh.createFiles
    }
    Actors.stop()
  }

  def findOld[T <: BackupPart](file: File)(implicit manifest: Manifest[T]) : (Option[T], FileAttributes) = {
    val path = file.getAbsolutePath
    // if the file is in the map, no other file can have the same name. Therefore we remove it.
    val out = oldBackupFilesRemaining.remove(path)
    val fa = FileAttributes(file, options)
    if (out.isDefined && 
        // if the backup part is of the wrong type => return (None, fa)
        manifest.erasure.isAssignableFrom(out.get.getClass()) &&
        // if the file has attributes and the last modified date is different, return (None, fa)
        (out.get.attrs != null && !out.get.attrs.hasBeenModified(file))) {
      // backup part is correct and unchanged
      (Some(out.get.asInstanceOf[T]), fa)
    } else {
      changed = true
      (None, fa)
    }
  }
  
  def backupFolderDesc(file: File) = {
    findOld[FolderDescription](file) match {
      case (Some(x), _) => x
      case (None, fa) =>
        val out = new FolderDescription(file.getAbsolutePath(), fa)
        if (delta) {
          deltaSet += out
        }
        out
    }
  }
  
  def backupFile(file: File) = {
    import blockStrategy._
    def makeNew(fa: FileAttributes) = {
        printDeleted(s"File ${file.getName} is new / has changed, backing up")
	    var hash: Array[Byte] = null
	    var hashChain: Array[Byte] = null
	    var faSave = fa
	    if (!options.onlyIndex) {
	    	val fis = new FileInputStream(file)
	    	val md = options.getMessageDigest
		    var out = Buffer[Array[Byte]]()
		    
		    val blockHasher = new BlockOutputStream(options.blockSize.bytes.toInt, {
		      buf : Array[Byte] => 
		        
		        val hash = md.digest(buf)
		        out += hash
		        if (!blockExists(hash)) {
		          writeBlock(hash, buf)
		        }
		    })
		    val hos = new HashingOutputStream(options.getHashAlgorithm)
		    val sis = new SplitInputStream(fis, blockHasher::hos::Nil)
		    sis.readComplete
		    val hashList = out.reduce(_ ++ _)
		    hash = hos.out.get
		    hashChain = if (hashList.length == hash.length) {
		      null
		    } else {
		      val key = options.getMessageDigest.digest(hashList)
	  		  if (!hashChainMap.contains(key)) {
			      hashChainMap += ((key, hashList))
			      hashChainMapTemp += ((key, hashList))
	  		  } else {
	  		    val collision = !Arrays.equals(hashChainMap(key), hashList)
	  		    val keyS = encodeBase64(key)
	  		    if (collision) {
	  		      l.error(s"Found hash collision in a hash chain with $keyS!")
	  		    } else {
	  		      l.debug(s"Hash $keyS was already in HashChainMap")
	  		    }
	  		  }
		      key
		    }
	    } else {
	      faSave = null
	    }
        val out = new FileDescription(file.getAbsolutePath(), file.length(), hash, hashChain, fa)
        if (delta) {
          deltaSet += out
        }
        out
    }
    findOld[FileDescription](file) match {
      case (Some(x), _) => x
      case (None, fa) => makeNew(fa)
    }
  }
  
}

class RestoreHandler(options: RestoreOptions) extends BackupBaseHandler[RestoreOptions](options) {
  import Streams._
  import Utils._
  
  val relativeTo = options.relativeToFolder.getOrElse(options.restoreToFolder)
  var _setup = false
  
  def setup() {
    if (_setup)
      return
    _setup = true
    import scala.concurrent.duration._
    if (!backupProperties.exists()) {
      if (options.remote.enabled) {
    	val fut1 = options.configureRemoteHandler   
        val fut = (Actors.remoteManager ? DownloadFile(backupProperties)) (1 minutes)
        Await.ready(fut, 1 minutes)
      } else {
        throw new IllegalArgumentException("No backup found at "+options.backupFolder)
      }
    }
	loadBackupProperties()
	l.info("Downloading files from remote storage if needed")
	val fut2 = options.configureRemoteHandler
	val newFut = fut2 match {
	  case _ => (Actors.remoteManager ? DownloadMetadata)(30 minutes)
	}
	Await.ready(newFut, 30 minutes)
	l.info("Finished downloading metadata")
    importOldHashChains
  }
  
  def restoreFolder() {
	setup()
    val dest = options.restoreToFolder
    val relativeTo = options.relativeToFolder.getOrElse(options.restoreToFolder) 
    val f = new File(options.backupFolder, "files.db")
    val (foldersC, filesC) = oldBackupFiles.partition{case f: FolderDescription => true; case _ => false}
    val files = filesC.map(_.asInstanceOf[FileDescription])
    val folders = foldersC.map(_.asInstanceOf[FolderDescription])
    folders.foreach(x => restoreFolderDesc(x))
    files.foreach(restoreFileDesc(_))
    folders.foreach(x => restoreFolderDesc(x))
    blockStrategy.free()
    Actors.stop()
  }
  
  def restoreFolderDesc(fd: FolderDescription) {
    val restoredFile = new File(options.restoreToFolder,  fd.relativeTo(relativeTo))
    restoredFile.mkdirs()
    fd.applyAttrsTo(restoredFile)
  }
  
  def getInputStream(fd: FileDescription) : InputStream = {
    setup()
	val hashes = getHashChain(fd).grouped(options.getMessageDigest.getDigestLength()).toSeq
	new InputStream() {
	  val hashIterator = hashes.iterator
	  var buf : ByteArrayInputStream = new ByteArrayInputStream(blockStrategy.readBlock(hashIterator.next))
	  def read() = {
	    if (buf.available() > 0) {
	      buf.read()
	    } else if (hashIterator.hasNext) {
	        buf = new ByteArrayInputStream(blockStrategy.readBlock(hashIterator.next))
	        buf.read()
	    } else {
	      -1
	    }
	  }
	}
  }
  
  def restoreFileDesc(fd: FileDescription) {
    val restoredFile = new File(options.restoreToFolder, fd.relativeTo(relativeTo))
    if (restoredFile.exists()) {
      if (restoredFile.length() == fd.size && !fd.attrs.hasBeenModified(restoredFile)) {
        return
      }
      l.info(s"${restoredFile.length()} ${fd.size} ${fd.attrs} ${restoredFile.lastModified()}")
      l.info("File exists, but has been modified, so overwrite")
    }
    l.info("Restoring to "+restoredFile)
    val fos = new FileOutputStream(restoredFile)
    copy(getInputStream(fd), fos)
    if (restoredFile.length() != fd.size) {
      l.error(s"Restoring failed for $restoredFile (old size: ${fd.size}, now: ${restoredFile.length()}")
    }
    fd.applyAttrsTo(restoredFile)
  }

}

class SearchHandler(findOptions: FindOptions, findDuplicatesOptions: FindDuplicateOptions) 
	extends BackupBaseHandler[BackupFolderOption](if (findOptions != null) findOptions else findDuplicatesOptions) {
  import Utils._
    
  def searchForPattern(x: String) {
    loadBackupProperties()
    val options = findOptions
    if (options == null) {
      throw new IllegalArgumentException("findOptions have to be defined")
    }
    l.info("Loading information")
    val loading = oldBackupFiles
    l.info(s"Information loaded (${loading.size} files), filtering")
    // TODO choose correct collection for sorting etc
    val filtered = loading.filter(_.path.contains(options.filePattern)).toList
    l.info(s"Filtering done, found ${filtered.size} entries")
    val sorted = if (filtered.size < 10000) filtered.sortBy(-_.size) else filtered
    sorted.take(100).foreach(printFile)
    if (filtered.size > 100) println("(Only listing 100 results)")
    val size = filtered.map(_.size).fold(0L)(_+_)
    l.info(s"Total ${readableFileSize(size)} found in ${filtered.size} files")
  }
  
  def printFile(x: BackupPart) {
    import org.fusesource.jansi.Ansi._
    import ConsoleManager._
    println(x match {
      case FileDescription(path, size, _, _, _) =>
        s"File: ${mark(path, findOptions.filePattern)} ${readableFileSize(size)}"
      case FolderDescription(path, _) => 
        s"Folder: ${mark(path, findOptions.filePattern)} "
    })
  }
  
  implicit class RichString(s: String){
    def fileLength = new File(s).length()
  }
  
  def findDuplicates() {
    loadBackupProperties()
	val options = findDuplicatesOptions
    if (options == null) {
      throw new IllegalArgumentException("findOptions have to be defined")
    }
    val size = options.minSize.bytes
    var bag = new HashMap[Long, Set[String]] {
      override def default(x: Long) = {
        Set()
      }
    }
    l.info("Loading")
    l.info("Found "+oldBackupFiles.size+" files in index")
    val set = oldBackupFiles.map(_.path.toLowerCase()).toSet
    val filesFiltered = oldBackupFiles.flatMap{case x: FileDescription => Some(x); case _ => None}.filter(_.size >= size)
    l.info("Found "+filesFiltered.size+s" files that are larger than ${options.minSize}")
    filesFiltered.foreach(x => bag.update(x.size, bag(x.size)++Buffer(x.path)))
    l.info("There are "+bag.size+" size groups")
    val duplicates = bag.values.map(_.filter(new File(_).exists)).filter(_.size > 1).toArray.sortBy(_.head.fileLength).reverse
    l.info("There are "+duplicates.size+" size groups with more than one file in it")
    var rest = duplicates
    var totalSize = new Size(duplicates.map(x => x.map(_.fileLength).sum).sum)
    var doneSize = 0L
    while (!rest.isEmpty) {
      val curSize = new Size(rest.head.head.fileLength)
      val duplicates = rest.take(100).map(sizeGroup => handleDuplicates(compareFiles(sizeGroup)))
      doneSize = rest.take(100).map(x => x.map(_.fileLength).sum).sum
      rest = rest.drop(100)
      l.info(s"Analyzed ${duplicates.size - rest.size}/${duplicates.size} files (${new Size(size)}/$totalSize)")
   }
  }
  
  
  /**
   * Helper class to keep the state for reading a 
   * file chunk-wise.
   */
  class CompareHelper(val f: String, bufSize: Int = 10240, skip: Int = 1024*1024) {
    val file = new File(f)
    if (!file.exists) {
      throw new IllegalArgumentException("File "+f+" does not exist")
    }
    val stream = new BufferedInputStream(new FileInputStream(file))
    var buf = ObjectPools.byteArrayPool.get(bufSize)
    var last : BAWrapper2 = null
    var read = 1
    def finished = read < 0
    def readNext() {
      read = stream.read(buf)
      stream.skip(skip)
      last = new BAWrapper2(buf)
    }
    def close() {
      stream.close()
      ObjectPools.byteArrayPool.recycle(buf)
      buf = null
    }
  }

  /**
   * Compares files of the same size chunk by chunk.
   * Executes the actions with the found duplicates. 
   */
  def compareFiles(files: Iterable[String]) = {
    val size = new Size(files.map(_.fileLength).sum)
    l.info("Starting to analyze group of "+files.size+" "+size+" "+"(files: "+files+")")
    var results = Buffer[Iterable[String]]() 
    val helpers = files.map(f => new CompareHelper(f.toLowerCase()))
    
    def compareHelpers(x: Iterable[CompareHelper]) {
      var remaining : Iterable[Iterable[CompareHelper]]= List(x)
      // while there is only one group, just keep comparing chunks
      while (remaining.size == 1) {
    	// if one file is finished, all of them are, they are the same size.
        if (remaining.head.exists(_.finished)) {
	      results += remaining.head.map(_.f)
	      return
	    }
        // group them according to content
	    val groups = helpers.map{ helper =>
	      helper.readNext
	      helper
	    }.groupBy{_.last}
	    // eliminate groups with only one in it
	    remaining = groups.values.filter(_.size>1)
      }
      // there is more than one group now, we need to recursively look at the different
      // groups here
	  remaining.foreach {compareHelpers _}
    }
    compareHelpers(helpers)
    helpers.map(_.close())
    results
  }
  
  val fileUtils = FileUtils.getInstance();
  
  def handleDuplicates(duplicateSets: Iterable[Iterable[String]]) = {
    val out = Buffer[File]()
    for (set <- duplicateSets) {
      l.info("Found duplicates "+set.mkString(" "))
      out ++= set.drop(1).map(x => new File(x))
      findDuplicatesOptions.action match {
        case DuplicateAction.delete => new DeleteAction().run(set)
        case DuplicateAction.moveToTrash => new MoveToTrashAction().run(set)
        case DuplicateAction.report =>
      }
    }
    out
  }
  
  def filterDuplicatesToDelete(files: Iterable[String]) : Iterable[File] = {
      files.toList
      	.filter{file => val pattern = findDuplicatesOptions.filePatternToKeep;
      		pattern == null || !file.contains(pattern)}
      	.filter(file => file.contains(findDuplicatesOptions.filePatternToDelete))
      	.map(new File(_))
  }
  
  abstract class Action(desc: String) {
	  def run(set: Iterable[String]) {
         val toDelete = filterDuplicatesToDelete(set)
         if (!toDelete.isEmpty) {
           val verb = if (findDuplicatesOptions.dryrun) "Would" else "Will"
           l.info(s"$verb $desc $toDelete")
           if (!findDuplicatesOptions.dryrun)
             completeAction(toDelete)
         }
	  }
	  def completeAction(toDelete: Iterable[File])
  }
  class DeleteAction extends Action("delete") {
    def completeAction(toDelete: Iterable[File]) {
      toDelete.foreach(_.delete)
    }
  }
  class MoveToTrashAction extends Action("delete") {
    def completeAction(toDelete: Iterable[File]) {
      fileUtils.moveToTrash(toDelete.toArray)
    }
  }
  
}

class CompactHandler(options: RestoreOptions) extends BackupBaseHandler[RestoreOptions](options) {
  import Streams._
  import Utils._
  implicit def long2Size(l: Long) = new Size(l)
  
  def calculateStorageOverhead {
    loadBackupProperties()
    val map = new HashChainMap()
    map ++= oldBackupHashChains
    oldBackupFiles.foreach { 
      case x: FileDescription if x.hashChain == null => map -= x.hash
      case x: FileDescription  => map -= x.hashChain
      case _ =>
    }
    l.info("Found "+map.size+" unused hash chains")
    val size: Size = map.values.map { x =>
      x.grouped(options.hashLength).map(blockStrategy.getBlockSize).sum
    }.sum
    println("Wasted in total "+size)
    println(blockStrategy.calculateOverhead(map.values))
  }
 
}
