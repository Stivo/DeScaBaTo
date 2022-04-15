package ch.descabato.core.model

import ch.descabato.utils.Hash
import com.google.protobuf.ByteString
import scalapb.TypeMapper


case class ChunkKey(hash: Hash) {

  override lazy val hashCode: Int = {
    hash.hashContent()
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case ChunkKey(other) =>
        hash === other
      case _ =>
        false
    }

  }

  override def toString: String = s"Chunk(${hash.toString})"
}

object ChunkKey {
  implicit val typeMapper: TypeMapper[ByteString, ChunkKey] =
    TypeMapper[ByteString, ChunkKey](x => ChunkKey(Hash(x.toByteArray)))(x => ByteString.copyFrom(x.hash.bytes))
}

case class RevisionKey(number: Int) extends AnyVal

object RevisionKey {
  implicit val typeMapper: TypeMapper[Int, RevisionKey] = TypeMapper(RevisionKey.apply)(_.number)
}


case class ValueLogStatusKey(name: String) extends AnyVal {
  def parseNumber: Int = {
    name.replaceAll("[^0-9]+", "").toInt
  }
}

object ValueLogStatusKey {
  implicit val typeMapper: TypeMapper[String, ValueLogStatusKey] = TypeMapper(ValueLogStatusKey.apply)(_.name)
}
