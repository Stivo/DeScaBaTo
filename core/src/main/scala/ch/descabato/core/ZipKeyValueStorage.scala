package ch.descabato.core

import ch.descabato.utils._
import ch.descabato.utils.Implicits._
import java.io.File
import net.java.truevfs.access.{TVFS, TFile}
import com.google.common.cache.{CacheLoader, RemovalNotification, RemovalListener, CacheBuilder}

// Threadsafe when reading, not when writing.
// Maximum 10 threads can concurrently access in a safe way.
// When writing, only use one thread or use this as an actor
trait ZipKeyValueStorage[K, V] extends UniversePart {
  // Needs to end with a slash, otherwise implementation will break
  def folder = ""

  def filetype: FileType[_]

  protected def lazyload = true

  @volatile
  protected var _loaded = false

  def writeTempFiles = false

  def keepValuesInMemory = false

  // Keeps all key values in memory if the above boolean is true
  var inMemoryCache: Map[K, V] = Map.empty
  // All files that are persisted
  var inBackupIndex: Map[K, File] = Map.empty
  var inCurrentWriterKeys: Set[K] = Set.empty

  def configureWriter(writer: ZipFileWriter) {}

  protected def convertStringToKey(x: String): K

  protected def convertKeyToString(x: K): String

  protected def readValue(name: String, reader: ZipFileReader): V

  protected def writeValueInto(name: String, v: V, zipFileWriter: ZipFileWriter)

  final def nameFor(k: K) = folder + convertKeyToString(k)

  protected var currentWriter: ZipFileWriter = null

  def load(files: Iterable[File]) {
    this.synchronized {
      files.foreach {
        f =>
          val num = filetype.numberOf(f)
          val reader = ZipFileHandlerFactory.reader(f, config)
          reader.names.foreach {
            name =>
              if (name.startsWith(folder)) {
                val keyname = name.substring(folder.length())
                val key = convertStringToKey(keyname)
                if (keepValuesInMemory) {
                  inMemoryCache += (key -> readValue(name, reader))
                }
                inCurrentWriterKeys += key
              }
          }
          reader.close()
          addKeysToIndex(f)
      }
    }
  }
  
  def load() {
    this.synchronized {
      val completeFiles = filetype.getFiles()
      val index = if (writeTempFiles) {
        completeFiles ++ filetype.getTempFiles()
      } else completeFiles
      load(index)
      _loaded = true
    }
  }

  protected def ensureLoaded() {
    if (lazyload && !_loaded) {
      this.synchronized {
        if (lazyload && !_loaded) {
          load
          _loaded = true
        }
      }
    }
  }

  protected def shouldStartNextFile(w: ZipFileWriter, k: K, v: V): Boolean

  def write(k: K, v: V) {
    ensureLoaded()
    if (keepValuesInMemory) {
      inMemoryCache += (k -> v)
    }
    if (shouldStartNextFile(currentWriter, k, v)) {
      endZipFile()
    }
    openZipFileWriter()
    val name = folder + convertKeyToString(k)
    writeValueInto(name, v, currentWriter)
    inCurrentWriterKeys += k
  }

  protected def openZipFileWriter() {
    if (currentWriter == null) {
      currentWriter = ZipFileHandlerFactory.writer(filetype.nextFile(temp = writeTempFiles), config)
      currentWriter.writeManifest(fileManager)
    }
  }

  def endZipFile() {
    if (currentWriter != null) {
      val file = currentWriter.file
      val num = filetype.numberOf(file)
      // close the file
      currentWriter.close
      // update journal
      universe.journalHandler.finishedFile(file, filetype)
      // create index
      addKeysToIndex(file)
      currentWriter = null
    }
  }

  def addKeysToIndex(file: File) {
    // add toAdd to inBackup or inBackupIndex
    inBackupIndex ++= inCurrentWriterKeys.map(x => (x, file))
    // reset toAdd
    inCurrentWriterKeys = Set.empty
  }
  
  // May only be called when not writing????
  def read(k: K): V = {
    ensureLoaded()
    // if values are loaded, check in inBackup and toAdd
    if (inMemoryCache safeContains k) {
      return inMemoryCache(k)
    }
    if (inBackupIndex safeContains k) {
      val out = readValue(nameFor(k), getZipFileReader(inBackupIndex(k)))
      if (keepValuesInMemory) {
        inMemoryCache += k -> out
      }
      return out
    }
    if (inCurrentWriterKeys safeContains k)
      throw new RuntimeException("Can not read values that are currently being added")
    throw new NoSuchElementException("Value is not in backup")
  }

  class CacheRemovalListener extends RemovalListener[File, ZipFileReader] {
    def onRemoval(notification: RemovalNotification[File, ZipFileReader]) {
      notification.getValue.close
    }
  }

  class ReaderCacheLoader extends CacheLoader[File, ZipFileReader] {
    def load(file: File) = {
      ZipFileHandlerFactory.reader(file, config)
    }
  }

  val cache = CacheBuilder.newBuilder().concurrencyLevel(5).maximumSize(20)
    .removalListener(new CacheRemovalListener)
    .build[File, ZipFileReader](new ReaderCacheLoader())

  protected def getZipFileReader(file: File) = cache.get(file)

  def exists(k: K): Boolean = {
    ensureLoaded()
    if ((inBackupIndex safeContains k) || (inCurrentWriterKeys safeContains k)) {
      true
    } else false
  }

  def isPersisted(k: K): Boolean = {
    ensureLoaded()
    inBackupIndex safeContains k
  }

  def finish(): Boolean = {
    endZipFile()
    cache.invalidateAll()
    true
  }

  protected def mergeFiles(from: Seq[File], to: File): Boolean = {
    val writer = ZipFileHandlerFactory.complexWriter(to)
    universe.journalHandler().createMarkerFile(writer, from)
    writer.writeManifest(fileManager)
    for (fromFile <- from) {
      writer.writeIntoFrom(fromFile, folder)
    }
    writer.close()
    universe.journalHandler().finishedFile(to, filetype, true)
    true
  }

}

trait IndexedKeyValueStorage[K, V] extends ZipKeyValueStorage[K, V] {
  
  def indexFileType: IndexFileType
  
  class IndexWriter(x: File) {
    val zipWriter = ZipFileHandlerFactory.writer(x, config)
    zipWriter.enableCompression
    zipWriter.writeManifest(universe.fileManager)
    val bos = zipWriter.newOutputStream("index")
    def close() {
      bos.close()
      zipWriter.close
    }
  }
  
  protected def convertKeyToBytes(x: K): Array[Byte]

  protected def convertBytesToKey(x: Array[Byte]): Iterator[K]
  
  override def load() {
    this.synchronized {
      if (useIndexFiles) {
        val indexes = indexFileType.getFiles()
        indexes.foreach { index =>
          val reader = ZipFileHandlerFactory.reader(index, config)
          val bytes = reader.getStream("index").readFully()
          inBackupIndex ++= convertBytesToKey(bytes).map(k => (k, indexFileType.fileForIndex(index)))
          reader.close
        }
        val indexNums = indexes.map(indexFileType.numberOf).toSet
        val volumeNums = filetype.getFiles().map(filetype.numberOf).toSet
        val volumesWithoutIndexes = indexNums -- volumeNums
        super.load(volumesWithoutIndexes.flatMap(filetype.fileForNumber(_, false)))
        _loaded = true
      } else {
        super.load()
      }
    }
  }
  
  def useIndexFiles = true

  override def addKeysToIndex(file: File) {
    if (useIndexFiles && !(indexFileType.indexForFile(file).exists()) && !(filetype.isTemp(file)))
      writeIndex(file)
    super.addKeysToIndex(file)
  }
  
  def writeIndex(file: File) {
    var dest = indexFileType.indexForFile(file)
    val writer = new IndexWriter(dest)
    for (k <- inCurrentWriterKeys) {
      writer.bos.write(convertKeyToBytes(k))
    }
    writer.close
    universe.journalHandler.finishedFile(dest, indexFileType, false)
  }
  
}

abstract class StandardZipKeyValueStorage extends ZipKeyValueStorage[BAWrapper2, Array[Byte]] 
		with IndexedKeyValueStorage[BAWrapper2, Array[Byte]] {
  
  protected def convertStringToKey(x: String) = {
    Utils.decodeBase64Url(x)
  }

  protected def convertKeyToString(x: BAWrapper2) = {
    Utils.encodeBase64Url(x.data)
  }

  protected def readValue(name: String, reader: ZipFileReader) = {
    reader.getStream(name).readFully
  }

  def readValueAsStream(k: BAWrapper2) = {
    getZipFileReader(inBackupIndex(k)).getStream(nameFor(k))
  }

  protected def convertKeyToBytes(x: BAWrapper2) = x.data

  protected def convertBytesToKey(bytes: Array[Byte]): Iterator[BAWrapper2] = {
    bytes.grouped(universe.config.hashLength).map {
      x => x : BAWrapper2
    }
  }

  protected def writeValueInto(name: String, v: Array[Byte], zipFileWriter: ZipFileWriter) = {
    zipFileWriter.writeEntry(name) {
      out =>
        out.write(v)
    }
  }

}
