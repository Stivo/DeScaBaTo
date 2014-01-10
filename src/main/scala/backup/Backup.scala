package backup

import java.io.FileInputStream
import java.io.FileOutputStream
import java.text.SimpleDateFormat
import scala.collection.mutable.Buffer
import java.io.File
import java.util.Date
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.util.Comparator
import java.util.Arrays
import scala.collection.mutable.HashMap
import java.io.BufferedInputStream
import com.sun.jna.platform.FileUtils
import scala.reflect.ManifestFactory

class BackupBaseHandler[T <: BackupFolderOption](val folder: T) extends DelegateSerialization with Utils with CountingFileManager {
  import Streams._
  import Test._
  import ByteHandling._
  import BAWrapper2.byteArrayToWrapper
  
  type HashChainMap = HashMap[BAWrapper2, Array[Byte]]
  
  val blockStrategy = folder.getBlockStrategy
  
  def getMatchingFiles(prefix: String) : Iterable[File] = {
    val files = folder.backupFolder.listFiles().filter(_.isFile()).filter(_.getName().startsWith(prefix))
    val sorted = files.sortBy(_.getName())
    if (files.isEmpty) {
      Nil
    } else {
      val lastPattern = sorted.last.getName().takeWhile(_!= '_')
      val onlyLast = sorted.dropWhile(!_.getName().contains(lastPattern)).toList
      if (onlyLast.isEmpty)
        // there was only one backup, use it
        sorted.toList
      else
        onlyLast
    }
  }
  
  lazy val oldBackupFiles = {
    val filesToLoad = getMatchingFiles("files-")
    implicit val options = folder
    filesToLoad.map(readObject[Buffer[BackupPart]]).fold(Buffer[BackupPart]())(_ ++ _)
  }

  var nextHashChainNum = 0
  
  lazy val oldBackupHashChains = {
    val (filesToLoad, max) = getFilesAndNextNum(folder.backupFolder)
    nextHashChainNum = max
    implicit val options = folder
    val list = filesToLoad.map(readObject[Array[(BAWrapper2, Array[Byte])]]).fold(Array())(_ ++ _)
    val map = new HashChainMap
    map ++= list
    map
  }

  val s = new SimpleDateFormat("yyyy-MM-dd.HHmmss.SSS")
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
  
  val prefix = "hashchains_"
    
  val backupProperties = "backup.properties"
    
  def loadBackupProperties(file: File = new File(folder.backupFolder, backupProperties)) {
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
  import Test._
  import ByteHandling._
  
  var changed: Boolean = false
  
  var hashChainMapTemp : HashChainMap = new HashChainMap()
  
  val oldBackupFilesRemaining = HashMap[String, backup.BackupPart]()
  
  def backupFolder() {
    changed = false
    new File(options.backupFolder, "blocks").mkdirs()
    l.info("Starting to backup")
    val backupPropertyFile = new File(options.backupFolder, backupProperties)
    if (backupPropertyFile.exists) {
      l.info("Loading old backup properties")
      loadBackupProperties(backupPropertyFile)
    } else {
      serialization = folder.serialization
      l.info("Saving backup properties")
      options.saveConfigFile(backupPropertyFile)
    }
  	l.info(s"Found ${oldBackupFiles.size} previously backed up files")
  	oldBackupFilesRemaining ++= oldBackupFiles.map(x => (x.path, x))
  	importOldHashChains
    val now = new Date()
    val filename = s"${s.format(now)}"
    var (counter, fileCounter, sizeCounter) = (0, 0, 0L)
    val files = Buffer[BackupPart]()
    
    var filesWritten = Buffer[File]()
    
    def newFiles() {
      var filesName = s"files-${filename}_$counter.db"
      counter += 1
      fileCounter += files.length
      sizeCounter += files.map(_.size).fold(0L)(_+_)
      val write = new File(options.backupFolder, filesName)
      writeObject(files, write)(options, ManifestFactory.classType(files.getClass))
      filesWritten += write
      if (!hashChainMapTemp.isEmpty) {
	      var hashChainsName = s"hashchains_$nextHashChainNum.db"
	      nextHashChainNum += 1
	      writeObject(hashChainMapTemp.toArray, new File(options.backupFolder, hashChainsName))(options, ManifestFactory.classType(files.getClass))
      }
      files.clear
      hashChainMapTemp = new HashChainMap()
    }
    def walk(file: File) {
      if (files.length >= options.saveIndexEveryNFiles) {
        newFiles()
      }
      file.isDirectory() match {
        case true => files += backupFolderDesc(file); file.listFiles().foreach(walk)
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
    } 
    if (!changed) {
      l.info(s"Nothing has changed, removing this backup")
      filesWritten.foreach(_.delete)
    }
  }

  def findOld[T <: BackupPart](file: File)(implicit manifest: Manifest[T]) : (Option[T], FileAttributes) = {
    val path = file.getAbsolutePath
    // if the file is in the map, no other file can have the same name. Therefore we remove it.
    val out = oldBackupFilesRemaining.remove(path)
    val fa = FileAttributes(file)
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
      case (None, fa) => new FolderDescription(file.getAbsolutePath(), fa)
    }
  }
  
  def backupFile(file: File) = {
    import blockStrategy._
    printDeleted(s"Analyzing File ${file.getName}")
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
	    new FileDescription(file.getAbsolutePath(), file.length(), hash, hashChain, fa)
    }
    findOld[FileDescription](file) match {
      case (Some(x), _) => x
      case (None, fa) => makeNew(fa)
    }
  }
  
}

class RestoreHandler(options: RestoreOptions) extends BackupBaseHandler[RestoreOptions](options) {
  import Streams._
  import Test._
  import ByteHandling._
  
  val relativeTo = options.relativeToFolder.getOrElse(options.restoreToFolder)
      
  def restoreFolder() {
	loadBackupProperties()
    importOldHashChains
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
  }
  
  def restoreFolderDesc(fd: FolderDescription) {
    val restoredFile = new File(options.restoreToFolder,  fd.relativeTo(relativeTo))
    restoredFile.mkdirs()
    fd.applyAttrsTo(restoredFile)
  }
  
  def restoreFileDesc(fd: FileDescription) {
    val hashes = getHashChain(fd).grouped(options.getMessageDigest.getDigestLength())
    val restoredFile = new File(options.restoreToFolder, fd.relativeTo(relativeTo))
    l.info("Restoring to "+restoredFile)
    val fos = new FileOutputStream(restoredFile)
    for (x <- hashes) {
      l.trace(s"Restoring from block $x")
      fos.write(blockStrategy.readBlock(x))
    }
    fos.close()
    if (restoredFile.length() != fd.size) {
      l.error(s"Restoring failed for $restoredFile (old size: ${fd.size}, now: ${restoredFile.length()}")
    }
    fd.applyAttrsTo(restoredFile)
  }

}

class SearchHandler(findOptions: FindOptions, findDuplicatesOptions: FindDuplicateOptions) 
	extends BackupBaseHandler[BackupFolderOption](if (findOptions != null) findOptions else findDuplicatesOptions) {
  import ByteHandling._
    
  def searchForPattern(x: String) {
    loadBackupProperties()
    val options = findOptions
    if (options == null) {
      throw new IllegalArgumentException("findOptions have to be defined")
    }
    l.info("Loading information")
    val loading = oldBackupFiles
    l.info("Information loaded (${loading.size} files), filtering")
    val filtered = loading.filter(_.path.contains(options.filePattern))
    l.info(s"Filtering done, found ${filtered.size} entries")
    filtered.take(100).foreach(printFile)
    if (filtered.size > 100) println("(Only listing 100 results)")
    val size = filtered.map(_.size).fold(0L)(_+_)
    l.info(s"Total ${readableFileSize(size)} found in ${filtered.size} files")
  }
  
  def printFile(x: BackupPart) {
    import org.fusesource.jansi.Ansi._
    println (x match {
      case fd@FileDescription(path, size, _, _, _) => {
        val (parentPath, name) = (path.dropRight(fd.name.length), fd.name)
        ansi().a("File: ").fg(Color.RED).a(parentPath).fg(Color.WHITE).a(s"$name ${readableFileSize(size)}")
      }
      case FolderDescription(path, _) => ansi().fg(Color.RED).a("Folder: ").fg(Color.WHITE).a(path)
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
    l.info("Found "+oldBackupFiles.size+" files in index")
    val filesFiltered = oldBackupFiles.flatMap{case x: FileDescription => Some(x); case _ => None}.filter(_.size >= size)
    l.info("Found "+filesFiltered.size+s" files that are larger than ${options.minSize}")
    filesFiltered.foreach(x => bag.update(x.size, bag(x.size)++Buffer(x.path)))
    l.info("There are "+bag.size+" size groups")
    val duplicates = bag.values.filter(_.size > 1).toArray.sortBy(_.head.fileLength).reverse
    l.info("There are "+duplicates.size+" size groups with more than one file in it")
    var rest = duplicates
    var totalSize = new Size(duplicates.map(x => x.map(_.fileLength).sum).sum)
    var doneSize = 0L
    while (!rest.isEmpty) {
      val curSize = new Size(rest.head.head.fileLength)
      val duplicates = rest.take(100).map(compareFiles)
      doneSize = rest.take(100).map(x => x.map(_.fileLength).sum).sum
      rest = rest.drop(100)
      
      l.info(s"Analyzed ${duplicates.size - rest.size}/${duplicates.size} files (${new Size(size)}/$totalSize)")
   }
  }
  
  class CompareHelper(val f: String, bufSize: Int = 10240) {
    val file = new File(f)
    if (!file.exists) {
      throw new IllegalArgumentException("File "+f+" does not exist")
    }
    val stream = new BufferedInputStream(new FileInputStream(file))
    val buf = Array.ofDim[Byte](bufSize)
    // one round: compare the streams, group by them
    // then continue with those groups$
    var last : BAWrapper2 = null
    def finished = if (last != null) last.length < 0 else false
    def readNext() = {
      val read = stream.read(buf)
      last = new BAWrapper2(buf, read)
      read < 0
    }
  }
  
  def compareFiles(files: Iterable[String]) {
    printDeleted("Starting to analyze group of "+files.size+" files: "+files)
    var results = Buffer[Iterable[String]]() 
    val helpers = files.map(f => new CompareHelper(f.toLowerCase()))
    val fileUtils = FileUtils.getInstance();
    def compareHelpers(x: Iterable[CompareHelper]) {
      var remaining : Iterable[Iterable[CompareHelper]]= List(x)
      while (remaining.size == 1) {
        if (remaining.head.exists(_.finished)) {
	      results += remaining.head.map(_.f)
	      return
	    }
	    val groups = helpers.map{ helper =>
	      helper.readNext
	      helper
	    }.groupBy{_.last}
	    remaining = groups.values.filter(_.size>1)
      }
	  remaining.foreach {compareHelpers _}
    }
    compareHelpers(helpers)
    helpers.map(_.stream.close())
    def filterDuplicatesToDelete(files: Iterable[String]) : Iterable[File] = {
      files.toList
      	.filter{file => val pattern = findDuplicatesOptions.filePatternToKeep;
      		pattern == null || !file.contains(pattern)}
      	.filter(file => file.contains(findDuplicatesOptions.filePatternToDelete))
      	.map(new File(_))
    }
    for (set <- results) {
      l.info("Found duplicates "+set.mkString(" "))
      abstract class Action(desc: String) {
    	  def run {
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
      findDuplicatesOptions.action match {
        case DuplicateAction.delete => new DeleteAction().run
        case DuplicateAction.moveToTrash => new MoveToTrashAction().run
        case DuplicateAction.report =>
      }
    }
  }
  
}