package ch.descabato.core.util

import java.io.{ByteArrayInputStream, OutputStream}

import akka.util.ByteString

object Implicits {
  import scala.language.higherKinds

  //  implicit def hashToWrapper(a: Hash): BytesWrapper = new BytesWrapper(a.bytes)
//  implicit def hashToArray(a: Hash): Array[Byte] = a.bytes
//  implicit class AwareMessageDigest(md: MessageDigest) {
//    def update(bytesWrapper: BytesWrapper): Unit = {
//      md.update(bytesWrapper.array, bytesWrapper.offset, bytesWrapper.length)
//    }
//    def finish(bytesWrapper: BytesWrapper): Hash = {
//      update(bytesWrapper)
//      finish()
//    }
//    def finish(): Hash = {
//      new Hash(md.digest())
//    }
//  }

//  implicit class ByteArrayUtils(buf: Array[Byte]) extends RealEquality[Array[Byte]]{
//    def ===(other: Array[Byte]): Boolean = java.util.Arrays.equals(buf, other)
//    def wrap(): BytesWrapper = new BytesWrapper(buf)
//  }

  implicit class InvariantContains[T, CC[X] <: Seq[X]](xs: CC[T]) {
    def safeContains(x: T): Boolean = xs contains x
  }
  implicit class InvariantContains2[T, CC[X] <: scala.collection.Set[X]](xs: CC[T]) {
    def safeContains(x: T): Boolean = xs contains x
  }
  implicit class InvariantContains3[T](xs: scala.collection.Map[T, _]) {
    def safeContains(x: T): Boolean = xs.keySet contains x
  }

 }
