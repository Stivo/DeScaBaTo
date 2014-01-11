package backup

import java.io.InputStream
import java.io.File
import java.io.FileInputStream
import java.nio.charset.Charset
import java.util.Arrays
import java.io.IOException
import scala.collection.mutable
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.nio.ByteOrder
import scala.collection.mutable.Buffer

/**
 * Creates par2 files for the backup folder that cover the
 * index, the hash chains and the volumes.
 */
class RedundancyHandler(folder: File, options: RedundancyOptions) extends CountingFileManager with Utils {
	def readCoveredFiles : Set[File] = {
	    val list = folder.listFiles
	    			.filter(_.isFile).filter(_.getName().endsWith(".par2"))
	    list.map(f => new Par2Parser(f).parse()).fold(List())(_ ++ _).toSet
    }
    
    def createFiles {
	    forHashChainsAndIndex
	    forVolumes
    } 
    
    var numberOfFiles = 1
    
    var prefix = ""
      
    def notCovered = {
      val covered = readCoveredFiles
      val out = folder.listFiles().filter(_.isFile())
      	.filter(!_.getName().endsWith(".par2"))
      	.filter(x => !(covered contains x))
      out
    }
    
    def forVolumes {
      import options._
      val prefix = "volume_"
      var volumes = filesMatchingPrefix(prefix)
      while (!volumes.isEmpty) {
	    val num = getFilesAndNextNum(folder)(s"par_$prefix")._2
	    val f = new File(folder, s"par_$prefix$num.par2")
        start(f, volumes.take(volumesToParTogether))
        volumes = volumes.drop(volumesToParTogether)
      }
    }

    def filesMatchingPrefix(prefix: String, sort : Boolean = true) = { 
      val out = notCovered.filter{ file =>
        file.isFile() && 
        file.getName.toLowerCase().startsWith(prefix)
      }
      if (sort) {
    	out.sortBy(getNum(_)("par_"+prefix))
      } else {
        out
      }
    }
    
    def handleFiles(prefix: String) = {
      val files = filesMatchingPrefix(prefix)
      val num = getFilesAndNextNum(folder)("par_"+prefix)._2
      val par2File = new File(folder, s"par_$prefix$num.par2")
      start(par2File, files)
    }
    
    def forHashChainsAndIndex {
      val backup = options.percentage
      try {
	 	  options.percentage = 50
	      handleFiles("hashchains_")
	 	  handleFiles("files_")
      } finally {
        options.percentage = backup
      }
    }
    
    /**
     * Starts the command line utility to create the par2 files
     */
    def start(par2File: File, files: Iterable[File]) {
      import options._
      if (files.isEmpty) {
        return
      }
      l.info(s"Starting par2 creation for ${files.size} files ${new Size(files.map(_.length()).sum)})")
      l.info("This may take a while")
      val cmd = Buffer[String]()
      cmd += par2Executable.getAbsolutePath
      cmd += "create"
      cmd += s"-r$percentage"
      cmd += s"-n$numberOfFiles" 
      cmd += s"-s${blockSize.bytes}"
      cmd += par2File.getAbsolutePath()
      cmd += "--"
      cmd ++= files.map(_.getAbsolutePath()) 
      val proc = new ProcessBuilder().command(cmd : _*)
      	.redirectError(new File(par2File.getParentFile(), "par2log.txt"))
      	.start
      val exit = proc.waitFor()	
      if (exit == 0) {
        l.info("par2 creation ended successfully")
      } else {
        l.info("par2 creation failed")
      }
    }
      
}
  
/**
 * Parses a given par2 file to find out which 
 * files it covers.
 */
class Par2Parser(val f: File) {
    val raf = new RandomAccessFile(f, "r")
    import Utils._
    
    var files = mutable.Map[String, String]()
    
    val ascii = Charset.forName("ASCII")
    val utf = Charset.forName("UTF-16LE")
    val magic = "PAR2\0PKT".getBytes(ascii).toArray
    val fileDescHeader = "PAR 2.0\0FileDesc".getBytes(ascii)
    val fileDescHeaderUnicode = "PAR 2.0\0UniFileN".getBytes(ascii)
    
    def readBytes(bytes: Int) = {
      val out = Array.ofDim[Byte](bytes)
      raf.readFully(out)
      out
    }
    
    def readLong() = {
      val map = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, raf.getFilePointer(), 8);
      map.order(ByteOrder.LITTLE_ENDIAN);
      map.asLongBuffer().get()
    }
    
    class ParsingException(m: String) extends IOException(m+" at pos "+raf.getFilePointer())
    
    def arrayEq(a: Array[Byte], b: Array[Byte]) = Arrays.equals(a, b) 
    
    def parseOnePacket {
      var start = raf.getFilePointer()
      if (!arrayEq(readBytes(8), magic)) {
        throw new ParsingException("Expected header ")
      }
      val length = readLong()
      raf.skipBytes(40)
      val typ = readBytes(16)
      if (arrayEq(typ, fileDescHeader)) {
        val id = readBytes(16)
        raf.skipBytes(40)
        val stringLength = length+start- raf.getFilePointer()
        val name = new String(readBytes(stringLength.toInt), ascii).trim
        files(encodeBase64(id)) = name
        if (raf.getFilePointer() != length+start) {
          throw new ParsingException("Not at end of packet somehow")
        }
      } else if (arrayEq(typ, fileDescHeaderUnicode)) {
        val id = readBytes(16)
        val stringLength = length+start- raf.getFilePointer()
        val name = new String(readBytes(stringLength.toInt), utf).trim
        files(encodeBase64(id)) = name
        if (raf.getFilePointer() != length+start) {
          throw new ParsingException("Not at end of packet somehow")
        }
      } else {
        raf.seek(length+start)
      }
    }

    def parse() = {
      while (raf.length()> raf.getFilePointer())
    	  parseOnePacket
      raf.close()
      files.values.map(new File(f.getParentFile(), _))
	}
}
  
