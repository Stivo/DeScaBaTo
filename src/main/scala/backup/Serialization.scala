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
import de.undercouch.bson4jackson.BsonFactory
import ByteHandling._
import de.undercouch.bson4jackson.BsonGenerator
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.dataformat.smile.SmileGenerator
import com.fasterxml.jackson.dataformat.smile.SmileParser
import java.io.OutputStream
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector

trait Serialization {
  def writeObject[T](t: T, out: OutputStream)(implicit m: Manifest[T]) : Unit
  
  def writeObject[T](t: T, file: File)(implicit options: FileHandlingOptions, m: Manifest[T]) {
    writeObject(t, newFileOutputStream(file))
  }
  def readObject[T](file: File)(implicit options: FileHandlingOptions, m: Manifest[T]) : T 
}

trait DelegateSerialization extends Serialization {
  var serialization: Serialization = null
  def writeObject[T](t: T, out: OutputStream)(implicit m: Manifest[T]) {
    serialization.writeObject(t, out)
  }
    
  def readObject[T](file: File)(implicit options: FileHandlingOptions, m: Manifest[T]) =
    serialization.readObject(file)
}

abstract class AbstractJacksonSerialization extends Serialization {
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

  val testModule = new SimpleModule("MyModule", new Version(1, 0, 0, null));
  testModule.addDeserializer(classOf[BackupPart], new BackupPartDeserializer())
  testModule.addDeserializer(classOf[BAWrapper2], new BAWrapper2Deserializer())
  testModule.addSerializer(classOf[BAWrapper2], new BAWrapper2Serializer())

  def mapper : ObjectMapper with ScalaObjectMapper
  
  mapper.registerModule(DefaultScalaModule)
  
  mapper.registerModule(testModule)
  mapper.enable(SerializationFeature.INDENT_OUTPUT);
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  
  def writeObject[T](t: T, out: OutputStream)(implicit m: Manifest[T]) {
    mapper.writeValue(out, t)
  } 
  
  def readObject[T](file: File)(implicit options: FileHandlingOptions, m: Manifest[T]) = {
    mapper.readValue(newFileInputStream(file))
  }
  
}

class JsonSerialization extends AbstractJacksonSerialization {
  lazy val mapper = new ObjectMapper() with ScalaObjectMapper
}

class BsonSerialization extends AbstractJacksonSerialization {

  lazy val fac = {
    val out = new BsonFactory
    out.enable(BsonGenerator.Feature.ENABLE_STREAMING);
    out
  }
  
  lazy val mapper = new ObjectMapper(fac) with ScalaObjectMapper
  override def readObject[T](file: File)(implicit options: FileHandlingOptions, m: Manifest[T]) = {
    val out = super.readObject(file)
    val bools = Set("archive", "readonly", "hidden")
    out match {
      case x: FileDescription => {
        x.attrs.asScala.filterKeys(_.endsWith("Time")).foreach {
          case (k ,v) => x.attrs.put(k, v.toString.toLong)
        }
        x.attrs.asScala.filterKeys(bools contains).foreach {
          case (k ,v) => x.attrs.put(k, v.toString.toBoolean)
        }
      }
      case _ => 
    }
    out
  }
}

class SmileSerialization extends AbstractJacksonSerialization {

  lazy val fac = {
    val out = new SmileFactory
    out.disable(SmileParser.Feature.REQUIRE_HEADER)
    out.disable(SmileGenerator.Feature.WRITE_HEADER)
    out
  }
  
  lazy val mapper = new ObjectMapper(fac) with ScalaObjectMapper
}

class XmlSerialization extends AbstractJacksonSerialization {
  lazy val module = new JacksonXmlModule {
    setDefaultUseWrapper(true)
  }
  lazy val inspector = new JacksonAnnotationIntrospector
  lazy val mapper = new XmlMapper(module) with ScalaObjectMapper {
    setAnnotationIntrospector(inspector)
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
  
  def writeObject[T](t: T, out: OutputStream)(implicit m: Manifest[T]) {
    val kryo = getKryo()
    val output = new Output(out);
    try{ kryo.writeObject(output, t); }
    finally { output.close(); }
  }
  
  def readObject[T](file: File)(implicit options: FileHandlingOptions, m: Manifest[T]) = {
    val kryo = getKryo()
    val output = new Input(newFileInputStream(file));
    try{ kryo.readObject(output, m.erasure).asInstanceOf[T] }
    finally { output.close() }
  }
}