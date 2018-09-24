package com.tmalaska.flinktraining.example.wordcount

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.types.Row


/**
  * Created by tmalaska on 10/30/17.
  */
object StreamingSQL {
  def main(args:Array[String]): Unit = {
    val kafkaServerURL = args(0)
    val kafkaServerPort = args(1)
    val kafkaTopic = args(2)
    val groupId = args(3)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val properties = new Properties
    properties.setProperty("bootstrap.servers", kafkaServerURL + ":" + kafkaServerPort)
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", groupId)

    println("kafkaTopic:" + kafkaTopic)

    val entityCountStream:DataStream[(String, Int)] = env.addSource(
      new FlinkKafkaConsumer010(kafkaTopic, new SimpleStringSchema(), properties))
      .flatMap(line => line.toUpperCase.split(' '))
      .map(word => (word, 1))

    tableEnv.registerDataStream("myTable2", entityCountStream, 'word, 'frequency)

    val roleUp: Table = tableEnv.sql("SELECT word, SUM(frequency) FROM myTable2 GROUP BY word")

    roleUp.addSink(new CustomSinkFunction)

  }
}

class CustomSinkFunction() extends SinkFunction[Row] {
  @throws[Exception]
  def invoke(value: Row): Unit = {
    //Do something
    println("-" + value)

  }
}
