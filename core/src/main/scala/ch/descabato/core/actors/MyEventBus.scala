package ch.descabato.core.actors

import java.io.File

import akka.actor.ActorRef
import akka.event.{EventBus, LookupClassification}
import ch.descabato.core.util.FileType
import ch.descabato.utils.Hash

class MyEventBus extends EventBus with LookupClassification {
  type Event = MyEvent
  type Classifier = String
  type Subscriber = MySubscriber

  override protected def mapSize(): Int = 128

  override protected def compareSubscribers(a: Subscriber, b: Subscriber): Int = a.actorRef.compareTo(b.actorRef)

  override protected def classify(event: MyEvent): String = event.topic

  override protected def publish(event: MyEvent, subscriber: Subscriber): Unit = {
    subscriber.myEventReceiver.receive(event)
  }
}

case class MySubscriber(actorRef: ActorRef, myEventReceiver: MyEventReceiver)

object MyEvent {
  val globalTopic: String = "AllEvents"
}

trait MyEvent {
  def topic: String = MyEvent.globalTopic
}

case class FileAddedToJournal(filetype: FileType, file: File, md5hash: Hash) extends MyEvent

case class CheckpointedChunks(ids: Set[Long]) extends MyEvent

trait MyEventReceiver {
  def receive(myEvent: MyEvent): Unit
}
