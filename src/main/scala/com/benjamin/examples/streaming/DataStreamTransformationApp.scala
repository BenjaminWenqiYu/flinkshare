package com.benjamin.examples.streaming

import java.{lang, util}

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2019-07-22
  * Time: 14:25 
  * Description: DataStream转换算子
  */
object DataStreamTransformationApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    filterFunction(env)

    env.execute("DataStreamTransformationApp")
  }

  /**
    * dataStream 流处理 split和select算子结合使用
    * @param env
    * @return
    */
  def splitSelectFunction(env: StreamExecutionEnvironment): DataStreamSink[Long] = {
    import org.apache.flink.api.scala._
    val data = env.addSource(new CustomNonParallelSourceFunction)
    val splits = data.split(new OutputSelector[Long] {
      override def select(value: Long): lang.Iterable[String] = {
        val list = new util.ArrayList[String]()
        if (value % 2 == 0) {
          list.add("even")
        } else {
          list.add("odd")
        }
        list
      }
    })
    splits.select("even").print().setParallelism(1)
  }

  /**
    * dataStream 流处理 union算子
    * @param env
    * @return
    */
  def unionFunction(env: StreamExecutionEnvironment): DataStreamSink[Long] = {
    import org.apache.flink.api.scala._
    val data1 = env.addSource(new CustomNonParallelSourceFunction)
    val data2 = env.addSource(new CustomNonParallelSourceFunction)
    data1.union(data2).print().setParallelism(1)
  }

  /**
    * dataStream 流处理 filter算子
    * @param env
    * @return
    */
  def filterFunction(env: StreamExecutionEnvironment): DataStreamSink[Long] = {
    import org.apache.flink.api.scala._
    val data = env.addSource(new CustomNonParallelSourceFunction)
    data.map(x => x).filter(_ % 2 == 0).print().setParallelism(1)
  }
}

/**
  * 自定义source不能够并行
  */
class CustomNonParallelSourceFunction extends SourceFunction[Long] {
  var count = 1L
  var isRunning = true
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}

/**
  * 自定义source能够并行
  */
class CustomParallelSourceFunction extends ParallelSourceFunction[Long] {
  var count = 1L
  var isRunning = true
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
