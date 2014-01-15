package ch.descabato

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import java.io.File
import java.nio.file.attribute.FileTime
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
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.dataformat.smile.SmileGenerator
import com.fasterxml.jackson.dataformat.smile.SmileParser
import java.io.OutputStream
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector
import java.io.InputStream

trait Serialization {
  def writeObject[T](t: T, out: OutputStream)(implicit m: Manifest[T]): Unit

  //  def writeObject[T](t: T, file: File)(implicit options: FileHandlingOptions, m: Manifest[T]) {
  //    writeObject(t, Streams.newFileOutputStream(file))
  //  }
  def readObject[T](in: InputStream)(implicit m: Manifest[T]): T
}

trait DelegateSerialization extends Serialization {
  var serialization: Serialization = null
  def writeObject[T](t: T, out: OutputStream)(implicit m: Manifest[T]) {
    serialization.writeObject(t, out)
  }

  def readObject[T](in: InputStream)(implicit m: Manifest[T]) =
    serialization.readObject(in)
}

abstract class AbstractJacksonSerialization extends Serialization {
  class UpdatePartDeserializer extends StdDeserializer[UpdatePart](classOf[UpdatePart]) {
    def deserialize(jp: JsonParser, ctx: DeserializationContext) = {
      val mapper = jp.getCodec().asInstanceOf[ObjectMapper];
      val root = mapper.readTree(jp).asInstanceOf[ObjectNode];
      if (root.fieldNames().asScala.contains("folder")) {
        mapper.convertValue(root, classOf[FileDeleted])
      } else if (root.fieldNames().asScala.contains("hash")) {
        mapper.convertValue(root, classOf[FileDescription]);
      } else {
        mapper.convertValue(root, classOf[FolderDescription])
      }
    }
  }

  class BackupPartDeserializer extends StdDeserializer[BackupPart](classOf[BackupPart]) {
    def deserialize(jp: JsonParser, ctx: DeserializationContext) = {
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
    def deserialize(jp: JsonParser, ctx: DeserializationContext) = {
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
  testModule.addDeserializer(classOf[UpdatePart], new UpdatePartDeserializer())
  testModule.addDeserializer(classOf[BackupPart], new BackupPartDeserializer())
  testModule.addDeserializer(classOf[BAWrapper2], new BAWrapper2Deserializer())
  testModule.addSerializer(classOf[BAWrapper2], new BAWrapper2Serializer())

  def mapper: ObjectMapper with ScalaObjectMapper

  mapper.registerModule(DefaultScalaModule)

  mapper.registerModule(testModule)
  // TODO disable before a release
  mapper.enable(SerializationFeature.INDENT_OUTPUT);
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def writeObject[T](t: T, out: OutputStream)(implicit m: Manifest[T]) {
    mapper.writeValue(out, t)
    out.close()
  }

  def readObject[T](in: InputStream)(implicit m: Manifest[T]) = {
    mapper.readValue(in)
  }

}

class JsonSerialization extends AbstractJacksonSerialization {
  lazy val mapper = new ObjectMapper() with ScalaObjectMapper
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
