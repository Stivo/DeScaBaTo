package ch.descabato.core

import ch.descabato.core.PrefixMap.empty
import ch.descabato.core.PrefixMap.empty
import ch.descabato.core.PrefixMap.newBuilder
import ch.descabato.utils.Hash
import it.unimi.dsi.fastutil.Hash.Strategy
import it.unimi.dsi.fastutil.objects.AbstractObject2ObjectMap
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap

import java.util
import scala.collection.StrictOptimizedIterableOps
import scala.collection.mutable

class FastMap[V]
  extends mutable.Map[Hash, V]
    with mutable.MapOps[Hash, V, mutable.Map, FastMap[V]]
    with StrictOptimizedIterableOps[(Hash, V), mutable.Iterable, FastMap[V]] {

  private val s = new Strategy[Hash] {
    override def hashCode(o: Hash): Int = util.Arrays.hashCode(o.bytes)

    override def equals(a: Hash, b: Hash): Boolean = util.Arrays.equals(a.bytes, b.bytes)
  }

  private val underlying: AbstractObject2ObjectMap[Hash, V] = new Object2ObjectOpenCustomHashMap[Hash, V](s)

  override def iterator: Iterator[(Hash, V)] = {
    val underlyingIterator = underlying.keySet().iterator()
    new Iterator[(Hash, V)] :
      override def hasNext: Boolean = underlyingIterator.hasNext

      override def next(): (Hash, V) = {
        val n = underlyingIterator.next()
        val v = underlying.get(n)
        (n, v)
      }
  }

  override def subtractOne(elem: Hash): this.type = {
    underlying.remove(elem)
    this
  }

  override def get(key: Hash): Option[V] = {
    Option(underlying.get(key))
  }

  override def addOne(elem: (Hash, V)): this.type = {
    underlying.put(elem._1, elem._2)
    this
  }

  override def empty: FastMap[V] = new FastMap

  protected override def fromSpecific(coll: IterableOnce[(Hash, V)]): FastMap[V] = FastMap.from(coll)

  override protected def newSpecificBuilder: mutable.Builder[(Hash, V), FastMap[V]] = FastMap.newBuilder[V]

}


object FastMap {
  def empty[V] = new FastMap[V]

  def newBuilder[V]: mutable.Builder[(Hash, V), FastMap[V]] =
    new mutable.GrowableBuilder[(Hash, V), FastMap[V]](empty)

  def from[V](source: IterableOnce[(Hash, V)]): FastMap[V] =
    source match {
      case pm: FastMap[V] => pm
      case _ => (newBuilder[V] ++= source).result()
    }

}