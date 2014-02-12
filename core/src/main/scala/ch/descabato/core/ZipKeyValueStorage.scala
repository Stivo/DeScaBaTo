package ch.descabato.core

import ch.descabato.utils.ZipFileWriter
import ch.descabato.utils.ZipFileReader
import ch.descabato.utils.Implicits._
import ch.descabato.utils.ZipFileHandlerFactory
import ch.descabato.utils.Utils
import java.io.File
import net.java.truevfs.access.{TVFS, TFile}

// Not threadsafe! Use from single thread or as one-instance actor
// Only from one zip file may be read at the same time, at the moment
abstract class ZipKeyValueStorage[K, V] extends UniversePart {
  // Needs to end with a slash, otherwise implementation will break
  def folder = ""
  def filetype: FileType[_]
  protected def lazyload = true
  protected var _loaded = false
  def useIndexFiles = false
  // TODO implement this if needed
  def keepValuesInMemory = false
  def writeTempFiles = false
  var inBackup: Map[K, V] = Map.empty
  var inBackupIndex: Map[K, File] = Map.empty
  //var inCurrentWriter: Map[K, V] = Map.empty
  var inCurrentWriterKeys: Set[K] = Set.empty

  def configureWriter(writer: ZipFileWriter) {}

  protected def convertToKey(x: String): K
  protected def convertKey(x: K): String
  protected def readValue(name: String, reader: ZipFileReader): V
  protected def writeValueInto(name: String, v: V, zipFileWriter: ZipFileWriter)

  final def nameFor(k: K) = folder + convertKey(k)

  protected var currentWriter: ZipFileWriter = null

  def load() {
    val completeFiles = filetype.getFiles()
    val index = if (writeTempFiles) {
      completeFiles ++ filetype.getTempFiles()
    } else completeFiles
    index.foreach { f =>
      val num = filetype.num(f)
      val reader = ZipFileHandlerFactory.reader(f, config)
      reader.names.foreach { name =>
        if (name.startsWith(folder)) {
          val keyname = name.substring(folder.length())
          val key = convertToKey(keyname)
          if (keepValuesInMemory) {
            inBackup += (key -> readValue(name, reader))
          }
          inBackupIndex += (key -> f)
        }
      }
    }
    // TODO if index should be used but doesn't exist, create index
  }

  protected def ensureLoaded() {
    if (lazyload && !_loaded) {
      load
      _loaded = true
    }
  }

  protected def shouldStartNextFile(w: ZipFileWriter, k: K, v: V): Boolean

  def write(k: K, v: V) {
    ensureLoaded()
    if (shouldStartNextFile(currentWriter, k, v)) {
      endZipFile()
    }
    openZipFileWriter()
    val name = folder + convertKey(k)
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
      val num = filetype.num(file)
      // close the file
      currentWriter.close
      // update journal
      universe.journalHandler.finishedFile(file, filetype)
      // create an index if necessary
      if (useIndexFiles) {
        // TODO
      }
      // add toAdd to inBackup or inBackupIndex
      inBackupIndex ++= inCurrentWriterKeys.map(x => (x, file))
      // reset toAdd
      inCurrentWriterKeys = Set.empty
      currentWriter = null
    }
  }

  // May only be called when not writing????
  def read(k: K): V = {
    ensureLoaded()
    // if values are loaded, check in inBackup and toAdd
    if (inBackup safeContains k) {
      return inBackup(k)
    }
    if (inBackupIndex safeContains k) {
      return readValue(nameFor(k), getZipFileReader(inBackupIndex(k)))
    }
    if (inCurrentWriterKeys safeContains k)
      throw new RuntimeException("Can not read values that are currently being added")
    throw new NoSuchElementException("Value is not in backup")
  }

  protected var lastZip: Option[(File, ZipFileReader)] = None

  protected def getZipFileReader(file: File) = {
    lastZip match {
      case Some((f, zip)) if f == file => zip
      case _ =>
        lastZip.foreach { case (_, zip) => zip.close() }

        val out = ZipFileHandlerFactory.reader(file, config)
        lastZip = Some((file, out))
        out
    }
  }

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
    lastZip.foreach(_._2.close)
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

abstract class StandardZipKeyValueStorage extends ZipKeyValueStorage[BAWrapper2, Array[Byte]] {
  protected def convertToKey(x: String) = {
    Utils.decodeBase64Url(x)
  }
  protected def convertKey(x: BAWrapper2) = {
    Utils.encodeBase64Url(x.data)
  }
  protected def readValue(name: String, reader: ZipFileReader) = {
    reader.getStream(name).readFully
  }
  def readValueAsStream(k: BAWrapper2) = {
    getZipFileReader(inBackupIndex(k)).getStream(nameFor(k))
  }
  protected def writeValueInto(name: String, v: Array[Byte], zipFileWriter: ZipFileWriter) = {
    zipFileWriter.writeEntry(name) { out =>
      out.write(v)
    }
  }

}
