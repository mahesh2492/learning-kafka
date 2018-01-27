package kafka

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

/**
  * Created by knoldus on 27/1/18.
  */
class CustomConsumer {
  val config = ConfigFactory.load()
  private val properties = new Properties()

  //set the required properties for consumer
  properties.put("bootstrap.servers", config.getString("BOOTSTRAP_SERVER"))
  properties.put("group.id", "simple-kafka")
  properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](properties)

  def consumeFromKafka(topic: String) = {
    consumer.subscribe(java.util.Collections.singletonList(topic))
    while (true) {
      val records = consumer.poll(5000)
      for (record <- records.asScala)
        println(record.value())
    }
  }
}


object StartConsumer extends App {
  val topic = "topic-test"
  (new CustomConsumer).consumeFromKafka(topic)
}
