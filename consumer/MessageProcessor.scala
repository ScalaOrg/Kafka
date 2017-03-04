package com.clara.kafka.consumer

import com.twitter.util.Closable

trait  MessageProcessor[T] extends Runnable with Closable {
  def processBatch(messages: Seq[T]): Unit
}
