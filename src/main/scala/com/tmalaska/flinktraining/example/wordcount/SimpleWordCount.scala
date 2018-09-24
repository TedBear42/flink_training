package com.tmalaska.flinktraining.example.wordcount

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by tmalaska on 7/1/17.
  */
object SimpleWordCount {
  def main(args: Array[String]) {

    val kafkaServerURL = args(0)
    val kafkaServerPort = args(1)
    val kafkaTopic = args(2)
    val groupId = args(3)
    val typeOfWindow = args(4)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // create a stream using socket

    val properties = new Properties
    properties.setProperty("bootstrap.servers", kafkaServerURL + ":" + kafkaServerPort)
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", groupId)

    println("kafkaTopic:" + kafkaTopic)

    val wordCountStream:DataStream[String] = env.addSource(
      new FlinkKafkaConsumer010(kafkaTopic, new SimpleStringSchema(), properties))

    // implement word count
    val wordsStream = wordCountStream
      .flatMap(line => line.toUpperCase.split(' '))
      .map(word => (word, 1))
      //.flatMap{_.toUpperCase.split(' ')}
      //.map{ (_,1) }

    val keyValuePair = wordsStream.keyBy(0)

    val countPair = if (typeOfWindow.equals("slidingCount")) {
      //Slide by count.  Have a sliding window of 5 messages and trigger or slide 2 messages
      keyValuePair.countWindow(5, 2).sum(1)
    } else if (typeOfWindow.equals("tumbleTime")) {
      //Tumble by time.  Trigger and Slide by 5 seconds
      keyValuePair.timeWindow(new Time(5, TimeUnit.SECONDS)).sum(1)
    } else if (typeOfWindow.equals("slidingTime")) {
      //Slide by time.  Have a sliding window of 5 seconds that tiggers every 2 seconds
      keyValuePair.timeWindow(new Time(5, TimeUnit.SECONDS), new Time(2, TimeUnit.SECONDS)).sum(1)
    } else {
      //Tumble by time.  Trigger every 5 seconds
      keyValuePair.countWindow(5).sum(1)
    }

    // print the results

    countPair.print()

    // execute the program

    env.execute("Scala WordCount Example")

  }
}
