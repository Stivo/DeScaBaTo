package backup

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.twitter.chill.KryoSerializer
import java.io.File
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input
import java.nio.file.attribute.FileTime
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.Kryo
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import ch.qos.logback.core.spi.LogbackLock
import com.fasterxml.jackson.databind.DeserializationFeature
import scala.collection.JavaConverters._
import java.util.Date
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.SerializationFeature

import ByteHandling._

trait Serialization {
  def writeObject(a: Any, filename: File)(implicit options: FileHandlingOptions) : Unit
  def readObject[T](filename: File)(implicit m: Manifest[T], options: FileHandlingOptions) : T 
}

class DelegateSerialization(serialization: Serialization) extends Serialization {
  def writeObject(a: Any, filename: File)(implicit options: FileHandlingOptions) { 
    serialization.writeObject(a, filename)
  }
    
  def readObject[T](filename: File)(implicit m: Manifest[T], options: FileHandlingOptions) =
    serialization.readObject(filename)
}


class JsonSerialization extends Serialization {

  class BackupPartDeserializer extends StdDeserializer[BackupPart](classOf[BackupPart]) {
    def deserialize (jp: JsonParser, ctx: DeserializationContext) = {
      val mapper = jp.getCodec().asInstanceOf[ObjectMapper];  
      val root = mapper.readTree(jp).asInstanceOf[ObjectNode];
      if (root.fieldNames().asScala.contains("hash")) {
        mapper.convertValue(root, classOf[FileDescription]);  
      } else {
        mapper.convertValue(root, classOf[FolderDescription])
      }
    }
  }
  
  class BAWrapper2Deserializer extends StdDeserializer[BAWrapper2](classOf[BAWrapper2]) {
    def deserialize (jp: JsonParser, ctx: DeserializationContext) = {
      val bytes = jp.readValueAs(classOf[Array[Byte]])
      BAWrapper2.byteArrayToWrapper(bytes)
    }
  }
  class BAWrapper2Serializer extends StdSerializer[BAWrapper2](classOf[BAWrapper2]) {
      def serialize(ba: BAWrapper2, jg: JsonGenerator, prov: SerializerProvider): Unit = {
    	jg.writeBinary(ba.data)
      }
  }

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  val testModule = new SimpleModule("MyModule", new Version(1, 0, 0, null));
  testModule.addDeserializer(classOf[BackupPart], new BackupPartDeserializer())
  testModule.addDeserializer(classOf[BAWrapper2], new BAWrapper2Deserializer())
  testModule.addSerializer(classOf[BAWrapper2], new BAWrapper2Serializer())
  
  mapper.registerModule(testModule)
  mapper.enable(SerializationFeature.INDENT_OUTPUT);
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def writeObject(a: Any, file: File)(implicit options: FileHandlingOptions) {
    mapper.writeValue(newFileOutputStream(file), a)
  } 
    
  def readObject[T](file: File)(implicit m: Manifest[T], options: FileHandlingOptions) = {
    mapper.readValue(newFileInputStream(file))
  }
  
}

class KryoSerialization extends Serialization {
  import ByteHandling._
  
  def getKryo() = {
    val out = KryoSerializer.registered.newKryo
    out.register(classOf[FileTime], new Serializer[FileTime](){
      def write (kryo: Kryo, output: Output, fa: FileTime){
        kryo.writeObject(output, fa.toMillis())
      }
      def read (kryo: Kryo, input: Input, clazz: Class[FileTime]) = {
        val millis = kryo.readObject(input, classOf[Long])
        FileTime.fromMillis(millis)
      }
    })
    out.register(classOf[BAWrapper2], new Serializer[BAWrapper2](){
      def write (kryo: Kryo, output: Output, fa: BAWrapper2){
        kryo.writeObject(output, fa.data)
      }
      def read (kryo: Kryo, input: Input, clazz: Class[BAWrapper2]) = {
        val data = kryo.readObject(input, classOf[Array[Byte]])
        BAWrapper2.byteArrayToWrapper(data)
      }
    })
    out
  }
  
  def writeObject(a: Any, filename: File)(implicit options: FileHandlingOptions) {
    val kryo = getKryo()
    val output = new Output(newFileOutputStream(filename));
    try{ kryo.writeObject(output, a); }
    finally { output.close(); }
  }
  
  def readObject[T](filename: File)(implicit m: Manifest[T], options: FileHandlingOptions) : T = {
    val kryo = getKryo()
    val output = new Input(newFileInputStream(filename));
    try{ kryo.readObject(output, m.erasure).asInstanceOf[T] }
    finally { output.close() }
  }
}