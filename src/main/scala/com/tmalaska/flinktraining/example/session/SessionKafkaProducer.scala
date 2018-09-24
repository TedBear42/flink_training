package com.tmalaska.flinktraining.example.session

import java.util.{Properties, Random}

import net.liftweb.json.DefaultFormats
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import net.liftweb.json.Serialization.write

object SessionKafkaProducer {
  def main(args:Array[String]): Unit = {

    implicit val formats = DefaultFormats

    val kafkaServerURL = args(0)
    val kafkaServerPort = args(1)
    val topic = args(2)
    val numberOfEntities = args(3).toInt
    val numberOfMessagesPerEntity = args(4).toInt
    val waitTimeBetweenMessageBatch = args(5).toInt
    val chancesOfMissing = args(6).toInt

    val props = new Properties()
    props.put("bootstrap.servers", kafkaServerURL + ":" + kafkaServerPort)
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val r = new Random()
    var sentCount = 0

    println("About to send to " + topic)
    for (j <- 0 to numberOfMessagesPerEntity) {
      for (i <- 0 to numberOfEntities) {
        if (r.nextInt(chancesOfMissing) != 0) {
          val message = write(HeartBeat(i.toString, System.currentTimeMillis()))
          val producerRecord = new ProducerRecord[String,String](topic, message)
          producer.send(producerRecord)
          sentCount += 1
        }
      }
      println("Sent Count:" + sentCount)
      Thread.sleep(waitTimeBetweenMessageBatch)
    }

    producer.close()
  }
}
