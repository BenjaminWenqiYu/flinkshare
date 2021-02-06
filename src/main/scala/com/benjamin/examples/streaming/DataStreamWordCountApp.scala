package com.benjamin.examples.streaming

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2019-07-22
  * Time: 14:21 
  * Description:
  */
object DataStreamWordCountApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textDataStrea: DataStream[String] = env.socketTextStream("localhost", 9999)

    import org.apache.flink.api.scala._
    val counts = textDataStrea.flatMap { _.toLowerCase().split(" ") filter {_.nonEmpty}}
      .map{ (_, 1)}
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
    counts.print()

    env.execute("Window Stream WordCount")
  }
}
