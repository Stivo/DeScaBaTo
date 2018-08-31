package ch.descabato.utils

import java.io.{InputStream, OutputStream}

import ch.descabato.core.model._
import ch.descabato.utils.Implicits._
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, Version}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.dataformat.smile.{SmileFactory, SmileGenerator, SmileParser}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.collection.JavaConverters._

trait Serialization {
  def writeObject[T](t: T, out: OutputStream)(implicit m: Manifest[T]): Unit

  def readObject[T](in: InputStream)(implicit m: Manifest[T]) : Either[T, Exception] 
}

abstract class AbstractJacksonSerialization extends Serialization {
  def indent = true
  class UpdatePartDeserializer extends StdDeserializer[UpdatePart](classOf[UpdatePart]) {
    def deserialize(jp: JsonParser, ctx: DeserializationContext): UpdatePart = {
      val mapper = jp.getCodec().asInstanceOf[ObjectMapper]
      val root = mapper.readTree(jp).asInstanceOf[ObjectNode]
      val fields = root.fieldNames().asScala.toSet
      if (!fields.exists(_ == "attrs")) {
        mapper.convertValue(root, classOf[FileDeleted])
      } else if (fields.safeContains("hash")) {
        mapper.convertValue(root, classOf[FileDescription])
      } else if (fields.safeContains("linkTarget")) {
        mapper.convertValue(root, classOf[SymbolicLink])
      } else {
        mapper.convertValue(root, classOf[FolderDescription])
      }
    }
  }

  class BackupPartDeserializer extends StdDeserializer[BackupPart](classOf[BackupPart]) {
    def deserialize(jp: JsonParser, ctx: DeserializationContext): BackupPart = {
      val mapper = jp.getCodec().asInstanceOf[ObjectMapper]
      val root = mapper.readTree(jp).asInstanceOf[ObjectNode]
      if (root.fieldNames().asScala.contains("hash")) {
        mapper.convertValue(root, classOf[FileDescription])
      } else {
        mapper.convertValue(root, classOf[FolderDescription])
      }
    }
  }
  class BaWrapperDeserializer extends StdDeserializer[BytesWrapper](classOf[BytesWrapper]) {
    def deserialize(jp: JsonParser, ctx: DeserializationContext): BytesWrapper = {
      val bytes = jp.readValueAs(classOf[Array[Byte]])
      bytes.wrap()
    }
  }
  class BaWrapperSerializer extends StdSerializer[BytesWrapper](classOf[BytesWrapper]) {
    def serialize(ba: BytesWrapper, jg: JsonGenerator, prov: SerializerProvider): Unit = {
      jg.writeBinary(ba.asArray())
    }
  }

  val testModule = new SimpleModule("DeScaBaTo", new Version(0, 4, 0, null, "ch.descabato", "core"))
  testModule.addDeserializer(classOf[UpdatePart], new UpdatePartDeserializer())
  testModule.addDeserializer(classOf[BackupPart], new BackupPartDeserializer())
  testModule.addDeserializer(classOf[BytesWrapper], new BaWrapperDeserializer())
  testModule.addSerializer(classOf[BytesWrapper], new BaWrapperSerializer())

  def mapper: ObjectMapper with ScalaObjectMapper

  mapper.registerModule(DefaultScalaModule)

  mapper.registerModule(testModule)
  // TODO disable before a release
  if (indent) {
    mapper.enable(SerializationFeature.INDENT_OUTPUT)
  }
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def writeObject[T](t: T, out: OutputStream)(implicit m: Manifest[T]) {
    mapper.writeValue(out, t)
    out.close()
  }

  def write[T](t: T)(implicit m: Manifest[T]): Array[Byte] = {
    mapper.writeValueAsBytes(t)
  }

  def read[T](in: BytesWrapper)(implicit m: Manifest[T]): Left[T, Exception] = {
    Left(mapper.readValue(in.array, in.offset, in.length))
  }

  def readObject[T](in: InputStream)(implicit m: Manifest[T]) : Either[T, Exception] = {
    try {
      Left(mapper.readValue(in))
    } catch {
      case exception : Exception => Right(exception)
    }
  }

}

class JsonSerialization(override val indent: Boolean = true) extends AbstractJacksonSerialization {
  lazy val mapper = new ObjectMapper() with ScalaObjectMapper
}
