package ch.descabato.core

import scala.collection.mutable
import java.io.File
import ch.descabato.utils.Utils

trait EventBus[Event] extends LifeCycle {
  type Subscriber = PartialFunction[Event, Unit]

  def subscribe(x: Subscriber)

  def unsubscribe(x: Subscriber)

  def publish(e: Event)
  
  def load(): Unit = {} 
  def shutdown(): BlockingOperation = ret
  def finish(): Boolean = true
}

// A simple event bus. Not optimized, not thread safe
class SimpleEventBus[T] extends EventBus[T] with Utils {

  var subscribers: mutable.Buffer[Subscriber] = mutable.Buffer()

  def subscribe(x: Subscriber) {
    subscribers += x
  }

  def unsubscribe(x: Subscriber) {
    subscribers -= x
  }

  def publish(e: T) {
    //l.info("Publishing event "+e)
    subscribers.view.filter(_.isDefinedAt(e)).foreach(_.apply(e))
  }
}

trait ThreadSafeEventBus[T] extends EventBus[T] {
  val lock = new Object()

  abstract override def subscribe(x: Subscriber) {
    lock.synchronized {
      super.subscribe(x)
    }
  }

  abstract override def unsubscribe(x: Subscriber) {
    lock.synchronized {
      super.unsubscribe(x)
    }
  }

  abstract override def publish(e: T) {
    lock.synchronized {
      super.publish(e)
    }
  }

}

trait BackupEvent

case class VolumeFinished(f: File, savedBlocks: Set[BaWrapper]) extends BackupEvent

case class HashListCheckpointed(hashLists: Set[BaWrapper], savedBlocks: Set[BaWrapper]) extends BackupEvent

case object Add1CpuTask extends BackupEvent

case object Subtract1CpuTask extends BackupEvent