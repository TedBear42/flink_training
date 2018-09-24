package com.tmalaska.flinktraining.example.wordcount

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object MapWithStateWordCount {
  def main(args: Array[String]) {

    val kafkaServerURL = args(0)
    val kafkaServerPort = args(1)
    val kafkaTopic = args(2)
    val groupId = args(3)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // create a stream using socket

    val properties = new Properties
    properties.setProperty("bootstrap.servers", kafkaServerURL + ":" + kafkaServerPort)
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", groupId)

    println("kafkaTopic:" + kafkaTopic)

    val wordCountStream: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer010(kafkaTopic, new SimpleStringSchema(), properties))

    // implement word count

    val wordsStream = wordCountStream
      .flatMap(line => line.toUpperCase.split(' '))
      .map(word => (word,1))

    //Do a (control,B)
    val countPair = wordsStream.keyBy(0).mapWithState((in: (String, Int), count: Option[Int]) =>
      count match {
        case Some(c) => ( (in._1, c + in._2), Some(c + in._2) )
        case None => ( (in._1, in._2), Some(in._2) )
      })

    wordsStream.keyBy(0)

    countPair.print()

    // execute the program

    env.execute()

  }
}
