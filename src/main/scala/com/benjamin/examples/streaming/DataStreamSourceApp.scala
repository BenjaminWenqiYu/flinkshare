package com.benjamin.examples.streaming

import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2019-07-22
  * Time: 14:46 
  * Description: 流处理
  */
object DataStreamSourceApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    socketStream(env)
    nonParallelSourceFunction(env)
    richParallelSourceFunction(env)

    env.execute("DataStreamSourceApp")

  }

  /**
    * 自定义source 可以设置并行处理
    * @param env
    * @return
    */
  def richParallelSourceFunction(env: StreamExecutionEnvironment): DataStreamSink[Long] = {
    import org.apache.flink.api.scala._
    val data = env.addSource(new CustomParallelSourceFunction).setBufferTimeout(2)
    data.print()
  }

  def nonParallelSourceFunction(env: StreamExecutionEnvironment): DataStreamSink[Long] = {
    import org.apache.flink.api.scala._
    // data不能设置大于1的并行度
    val data = env.addSource(new CustomNonParallelSourceFunction)
    data.print()
  }

  def socketStream(env: StreamExecutionEnvironment) = {
    val textStream = env.socketTextStream("localhost", 9999)
    textStream.print().setParallelism(1)
  }
}
