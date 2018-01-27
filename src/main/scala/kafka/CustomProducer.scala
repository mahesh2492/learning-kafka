package kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Try

/**
  * Created by knoldus on 27/1/18.
  */
class CustomProducer {

  private val properties = new Properties()

  //set the required properties for producer
  properties.put("bootstrap.servers", "localhost:9092")
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](properties)

  def writetoKafka(topic: String) = {

    Try{
      for(i <- 1 to 125000)
        producer.send(new ProducerRecord[String, String](topic, i.toString, s"message-$i"))
    }.getOrElse(println("Exception has occurred while sending data to kafka topic"))

    println("Message  has been sent successfully")
    producer.close()
  }

}


object StartProducer extends App {
  val topic = "topic-test"
  (new CustomProducer).writetoKafka(topic)
}