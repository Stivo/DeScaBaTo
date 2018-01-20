package ch.descabato.core.util

import akka.util.ByteString
import ch.descabato.core.model.{Length, StoredChunk}
import ch.descabato.utils.{BytesWrapper, Hash}
import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator, JsonParser, Version}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JavaType, ObjectMapper, SerializerProvider}
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object Json {

  val mapper = new CustomObjectMapper()
  val smileMapper = new CustomObjectMapper(new SmileFactory())

  def main(args: Array[String]): Unit = {
    tryMap
  }

  private def tryMap = {
    var map: Map[Hash, StoredChunk] = Map.empty
    val hash = Hash("Hello".getBytes("UTF-8"))
    map += hash -> StoredChunk("Hello", hash, 0, Length(500))
    roundTrip(map.toSeq)
  }

  private def tryHash = {
    val hash = (5, Hash("Hello".getBytes("UTF-8")))
    roundTrip(hash)
  }

  private def roundTrip[T: Manifest](map: T) = {
    println(map)
    val json = mapper.writeValueAsString(map)
    println(json)
    val clazz = manifest[T].runtimeClass
    println(s"Class is $clazz")
    val deserialized = mapper.readValue[T](json)
    println(deserialized)
  }
}

class CustomObjectMapper(val jsonFactory: JsonFactory = new JsonFactory()) extends ObjectMapper(jsonFactory) with ScalaObjectMapper {

  override def constructType[T](implicit m: Manifest[T]): JavaType = {
    val ByteStringName = classOf[ByteString].getName
    m.runtimeClass.getName match {
      case ByteStringName => constructType(classOf[ByteString])
      case _ => super.constructType[T]
    }
  }

  registerModule(DefaultScalaModule)


  class BaWrapperDeserializer extends StdDeserializer[BytesWrapper](classOf[BytesWrapper]) {
    def deserialize(jp: JsonParser, ctx: DeserializationContext): BytesWrapper = {
      val bytes = jp.readValueAs(classOf[Array[Byte]])
      new BytesWrapper(bytes)
    }
  }

  class BaWrapperSerializer extends StdSerializer[BytesWrapper](classOf[BytesWrapper]) {
    def serialize(ba: BytesWrapper, jg: JsonGenerator, prov: SerializerProvider): Unit = {
      jg.writeBinary(ba.array, ba.offset, ba.length)
    }
  }

  class HashWrapperDeserializer extends StdDeserializer[Hash](classOf[Hash]) {
    def deserialize(jp: JsonParser, ctx: DeserializationContext): Hash = {
      val bytes = jp.readValueAs(classOf[Array[Byte]])
      Hash(bytes)
    }
  }

  class HashWrapperSerializer extends StdSerializer[Hash](classOf[Hash]) {
    def serialize(ba: Hash, jg: JsonGenerator, prov: SerializerProvider): Unit = {
      jg.writeBinary(ba.bytes)
    }
  }

  val testModule = new SimpleModule("DeScaBaTo", new Version(0, 5, 0, null, "ch.descabato", "core"))
  testModule.addDeserializer(classOf[BytesWrapper], new BaWrapperDeserializer())
  testModule.addSerializer(classOf[BytesWrapper], new BaWrapperSerializer())
  testModule.addDeserializer(classOf[Hash], new HashWrapperDeserializer())
  testModule.addSerializer(classOf[Hash], new HashWrapperSerializer())
  registerModule(testModule)
}