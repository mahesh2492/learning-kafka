package kafka

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Try

/**
  * Created by knoldus on 27/1/18.
  */
class CustomProducer {

  private val properties = new Properties()
  val config = ConfigFactory.load()

  //set the required properties for producer
  properties.put("bootstrap.servers", config.getString("BOOTSTRAP_SERVER"))
  properties.put("key.serializer", config.getString("SERIALIZER"))
  properties.put("value.serializer", config.getString("SERIALIZER"))

  val producer = new KafkaProducer[String, String](properties)

  def writetoKafka(topic: String): Unit = {

    Try{
      for(i <- 1 to 10000)
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