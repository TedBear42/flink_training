package com.tmalaska.flinktraining.example.wordcount

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object TriggerEvictWordCount {
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
      .map(word => (word, 1))

    //Link to Trigger API page https://ci.apache.org/projects/flink/flink-docs-release-1.3/api/java/org/apache/flink/streaming/api/windowing/triggers/Trigger.html
    //Link to Evictor API page https://ci.apache.org/projects/flink/flink-docs-release-1.3/api/java/org/apache/flink/streaming/api/windowing/evictors/Evictor.html

    val keyValuePair = wordsStream.keyBy(0).window(GlobalWindows.create()).
      trigger(CountTrigger.of(5)).evictor(CountEvictor.of(10))

    val countPair = keyValuePair.sum(1)

    // print the results

    countPair.print()

    // execute the program

    env.execute()
  }
}
