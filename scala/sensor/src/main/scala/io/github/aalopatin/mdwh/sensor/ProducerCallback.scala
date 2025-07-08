package io.github.aalopatin.mdwh.sensor

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

import java.time.LocalDateTime

class ProducerCallback extends Callback{
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null)
      exception.printStackTrace()
  }
}
