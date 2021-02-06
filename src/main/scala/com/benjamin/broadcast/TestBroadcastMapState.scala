package com.benjamin.broadcast

import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2021-02-06
  * Time: 18:29
  * Description: Non-Keyed Stream使用广播状态
  */
object TestBroadcastMapState {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    testBroadcastState(env)
  }

  def testBroadcastState(env: StreamExecutionEnvironment): Unit = {
    val ds1: DataStream[User] = env
      .readTextFile("E:/GitProject/flinkshare/flinkshare/src/main/resources/log1.txt")
      .map(s => {
        val arr = s.split(",")
        User(arr(0), arr(1))
      })
    val ds2: DataStream[Order] = env
      .readTextFile("E:/GitProject/flinkshare/flinkshare/src/main/resources/log2.txt")
      .map(s => {
        val arr = s.split(",")
        Order(arr(0), arr(1), arr(2).toInt)
      })
    val broadcastMapState: MapStateDescriptor[String, User] = new MapStateDescriptor[String, User]("broadcast",
      TypeInformation.of(classOf[String]), TypeInformation.of(classOf[User]))
    val broadcastDs: BroadcastStream[User] = ds1.broadcast(broadcastMapState)
    val connect: BroadcastConnectedStream[Order, User] = ds2.connect(broadcastDs)
    connect.process(new BroadcastProcessFunction[Order, User, Custom] {
      override def processElement(value: Order, readOnlyContext:
      BroadcastProcessFunction[Order, User, Custom]#ReadOnlyContext, collector: Collector[Custom]): Unit = {
        val broadcastState = readOnlyContext.getBroadcastState(broadcastMapState)
        val user = broadcastState.get(value.name)
        if (null != user) {
          val res = Custom(user.name, value.sex, user.job, value.age)
          collector.collect(res)
        }
      }

      override def processBroadcastElement(value: User, context:
      BroadcastProcessFunction[Order, User, Custom]#Context, collector: Collector[Custom]): Unit = {
        val broadcastState: BroadcastState[String, User] = context.getBroadcastState(broadcastMapState)
        broadcastState.put(value.name, value)
      }
    }).print()

    env.execute()
  }

}

case class User(name: String, job: String)

case class Order(name: String, sex: String, age: Int)

case class Custom(name: String, sex: String, job: String, age: Int)
