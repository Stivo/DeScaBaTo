package ch.descabato.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.DeserializationFeature
import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.dataformat.smile.SmileGenerator
import com.fasterxml.jackson.dataformat.smile.SmileParser
import java.io.{OutputStream, InputStream}
import ch.descabato.core.FileDeleted
import ch.descabato.core.FileDescription
import ch.descabato.core.BackupPart
import ch.descabato.core.UpdatePart
import ch.descabato.core.BaWrapper
import ch.descabato.core.SymbolicLink
import ch.descabato.core.FolderDescription
import ch.descabato.utils.Implicits._
import de.undercouch.bson4jackson.{BsonGenerator, BsonFactory}

trait Serialization {
  def writeObject[T](t: T, out: OutputStream)(implicit m: Manifest[T]): Unit

  def readObject[T](in: InputStream)(implicit m: Manifest[T]) : Either[T, Exception] 
}

abstract class AbstractJacksonSerialization extends Serialization {
  class UpdatePartDeserializer extends StdDeserializer[UpdatePart](classOf[UpdatePart]) {
    def deserialize(jp: JsonParser, ctx: DeserializationContext) = {
      val mapper = jp.getCodec().asInstanceOf[ObjectMapper];
      val root = mapper.readTree(jp).asInstanceOf[ObjectNode];
      val fields = root.fieldNames().asScala.toSet
      if (fields.find(_=="attrs").isEmpty) {
        mapper.convertValue(root, classOf[FileDeleted])
      } else if (fields.safeContains("hash")) {
        mapper.convertValue(root, classOf[FileDescription]);
      } else if (fields.safeContains("linkTarget")) {
        mapper.convertValue(root, classOf[SymbolicLink]);
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
  class BaWrapperDeserializer extends StdDeserializer[BaWrapper](classOf[BaWrapper]) {
    def deserialize(jp: JsonParser, ctx: DeserializationContext) = {
      val bytes = jp.readValueAs(classOf[Array[Byte]])
      bytes
    }
  }
  class BaWrapperSerializer extends StdSerializer[BaWrapper](classOf[BaWrapper]) {
    def serialize(ba: BaWrapper, jg: JsonGenerator, prov: SerializerProvider): Unit = {
      jg.writeBinary(ba.data)
    }
  }

  val testModule = new SimpleModule("MyModule", new Version(1, 0, 0, null));
  testModule.addDeserializer(classOf[UpdatePart], new UpdatePartDeserializer())
  testModule.addDeserializer(classOf[BackupPart], new BackupPartDeserializer())
  testModule.addDeserializer(classOf[BaWrapper], new BaWrapperDeserializer())
  testModule.addSerializer(classOf[BaWrapper], new BaWrapperSerializer())

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

  def readObject[T](in: InputStream)(implicit m: Manifest[T]) : Either[T, Exception] = {
    try {
      Left(mapper.readValue(in))
    } catch {
      case exception : Exception => Right(exception)
    }
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

class BsonSerialization extends AbstractJacksonSerialization {

  lazy val fac = {
    val out = new BsonFactory
    out.enable(BsonGenerator.Feature.ENABLE_STREAMING);
    out
  }

  lazy val mapper = new ObjectMapper(fac) with ScalaObjectMapper
}

