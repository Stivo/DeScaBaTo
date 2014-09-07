package ch.descabato.akka

import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.dispatch.UnboundedMailbox
import akka.dispatch._
import com.typesafe.config.Config

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.ref.WeakReference

object Queues {

  type Queue = UnboundedMailbox.MessageQueue

  private val map: mutable.Map[Option[ActorRef], WeakReference[Queue]] = mutable.HashMap()

  def += (x: Option[ActorRef], queue: Queue) {
    map(x) = WeakReference(queue)
  }

  def apply(actorRef: Option[ActorRef]): Option[Queue] = map.get(actorRef).map{case WeakReference(x) => x}
}

/**
 * MyBoundedMailbox
 */
case class MyMailbox(capacity: Int, pushTimeOut: FiniteDuration)
  extends MailboxType with ProducesMessageQueue[UnboundedMailbox.MessageQueue] {

  def this(settings: ActorSystem.Settings, config: Config) = this(config.getInt("mailbox-capacity"),
    Duration(config.getDuration("mailbox-push-timeout-time", TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS))

  if (capacity < 0) throw new IllegalArgumentException("The capacity for BoundedMailbox can not be negative")
  if (pushTimeOut eq null) throw new IllegalArgumentException("The push time-out for BoundedMailbox can not be null")

  final override def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = {
    val out = new UnboundedMailbox.MessageQueue()
    Queues += (owner, out)
    out
  }

}
