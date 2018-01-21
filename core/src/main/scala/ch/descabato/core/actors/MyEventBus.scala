package ch.descabato.core.actors

import akka.actor.ActorRef
import akka.event.{EventBus, LookupClassification}

import scala.concurrent.Future

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

case class VolumeRolled(filename: String) extends MyEvent

trait MyEventReceiver {
  def receive(myEvent: MyEvent): Unit
}