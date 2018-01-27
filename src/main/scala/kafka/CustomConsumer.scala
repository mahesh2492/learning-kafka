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
  properties.put("group.id", config.getString("GROUP_ID"))
  properties.put("auto.offset.reset", "earliest")
  properties.put("enable.auto.commit", "false")
  properties.put("key.deserializer", config.getString("DESERIALIZER"))
  properties.put("value.deserializer", config.getString("DESERIALIZER"))

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
