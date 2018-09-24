package com.tmalaska.flinktraining.example.session

import java.util.Properties

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.read
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._

object StreamingSessionExample {
  def main(args:Array[String]): Unit = {
    val kafkaServerURL = args(0)
    val kafkaServerPort = args(1)
    val kafkaTopic = args(2)
    val groupId = args(3)
    val sessionTimeOut = args(4).toInt

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //val socketStream = env.socketTextStream("localhost",9999, '\n')

    val properties = new Properties
    properties.setProperty("bootstrap.servers", kafkaServerURL + ":" + kafkaServerPort)
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", groupId)

    println("kafkaTopic:" + kafkaTopic)

    val messageStream:DataStream[String] = env.addSource(
      new FlinkKafkaConsumer010(kafkaTopic, new SimpleStringSchema(), properties))

    val heartBeatStream = messageStream
      .map(str => {
        implicit val formats = DefaultFormats
        //println("str:" + str)
        val hb = read[HeartBeat](str)
        (hb.entityId, hb.eventTime)
      }).keyBy(0).process(new MyProcessFunction(sessionTimeOut))

    heartBeatStream.map(session => {
      println("session:" + session)
      session
    })

    heartBeatStream.print()

    env.execute()
  }
}

class MyProcessFunction(sessionTimeOut:Int) extends ProcessFunction[(String,Long), SessionObj] {


  private var state:ValueState[SessionObj] = null


  override def open(parameters: Configuration): Unit = {
    state = getRuntimeContext.getState(new ValueStateDescriptor[SessionObj]("myState", classOf[SessionObj]))
  }

  override def processElement(value: (String, Long),
                              ctx: ProcessFunction[(String, Long), SessionObj]#Context,
                              out: Collector[SessionObj]): Unit = {
    val currentSession = state.value()
    var outBoundSessionRecord:SessionObj = null
    if (currentSession == null) {
      outBoundSessionRecord = SessionObj(value._2, value._2, 1)
    } else {
      outBoundSessionRecord = SessionObj(currentSession.startTime, value._2, currentSession.heartbeatCount + 1)

    }
    state.update(outBoundSessionRecord)
    out.collect(outBoundSessionRecord)
    ctx.timerService.registerEventTimeTimer(System.currentTimeMillis() + sessionTimeOut)
  }

  override def onTimer(timestamp: Long,
                       ctx: ProcessFunction[(String, Long), SessionObj]#OnTimerContext,
                       out: Collector[SessionObj]): Unit = {
    val result = state.value
    if (result != null && result.latestEndTime + sessionTimeOut < System.currentTimeMillis()) { // emit the state on timeout
      state.clear()
    }
  }
}

case class SessionObj(startTime:Long, latestEndTime:Long, heartbeatCount:Int)