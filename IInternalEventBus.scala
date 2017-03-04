package com.clara.kafka

import java.util.concurrent.ConcurrentHashMap

import com.clara.kafka.consumer.MessageSource
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Future, Time}

import scala.collection.mutable.Queue

/**
  * an process-internal message bus to be used for quick and dirty development.
  *
  * N.B. limitations: does not support inter-process messaging, and does not support fan-out (i.e. only one consumer can read a given message)
  *
  * @param enabledTopics - topics that this bus will operate for; this is necessary because of the asymmetry
  *     where `send` takes topic as an argument but `getBatch` does not - that's a result of the fact that this
  *     implements both `MessageSender` and `MessageSource`
  * @param statsReceiver
  * @tparam T - type of the message
  */
class InternalEventBus[T](
  enabledTopics: Seq[String],
  statsReceiver: StatsReceiver
) extends MessageSender[T] with MessageSource[T] {

  private[this] val hashMap = new ConcurrentHashMap[String, Queue[T]]

  override def send(topic: String)
    (message: => T): Future[Unit] = {

    if (enabledTopics.contains(topic)) {
      val currentQueue = Option(hashMap.get(topic)).getOrElse(Queue.empty)
      currentQueue.enqueue(message)
      hashMap.put(topic, currentQueue)
      println(s"sent message on topic $topic, queue now has ${currentQueue.size} items")
    }

    Future.Done
  }

  override def getBatch(): Seq[T] = {
    enabledTopics.flatMap { topic =>
      val queue = Option(hashMap.get(topic)).getOrElse(Queue.empty)
      val items = queue.dequeueAll(_ => true)
      if (items.size > 0) println(s"@@@ dequeued ${items.size} items, queue now has ${queue.size} items")
      items
    }
  }

  override def close(deadline: Time): Future[Unit] = Future.Unit
  override def interrupt(): Unit = ()
}
