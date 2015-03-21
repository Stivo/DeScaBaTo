package ch.descabato.utils

import java.util

import scala.collection.mutable
class ByteArrayKeyedMap[V] extends mutable.HashMap[Array[Byte], V] {

  override def elemHashCode(key: Array[Byte]) = {
    util.Arrays.hashCode(key)
  }
  override def elemEquals(key1: Array[Byte], key2: Array[Byte]) = {
    util.Arrays.equals(key1, key2)
  }
}