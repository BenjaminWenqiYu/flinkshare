package com.benjamin.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2021-02-01
  * Time: 17:25
  * Description: 以求和为例，实现托管ValueState状态
  */
class CountWindowAverage extends RichFlatMapFunction[(Long, Long), (Long, Long)] {
  // ValueStage的数据类型为(Long, Long) 元组
  private var sum: ValueState[(Long, Long)] = _

  override def flatMap(in: (Long, Long), out: Collector[(Long, Long)]): Unit = {
    // 读取状态值
    val tmpCurrentSum = sum.value()
    val currentSum = if (null != tmpCurrentSum) tmpCurrentSum else (0L, 0L)
    val newSum = (currentSum._1 + 1, currentSum._2 + in._2)
    // 更新状态
    sum.update(newSum)
    // 求和、计数，当数量为2时输出中间结果并清除状态
    if (newSum._1 >= 2) {
      out.collect((in._1, newSum._2))
      sum.clear()
    }
  }

  /**
    * 通过名称和状态的数据类型初始化描述符
    *
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    sum = getRuntimeContext.getState(
      new ValueStateDescriptor[(Long, Long)]("average", createTypeInformation[(Long, Long)])
    )
  }
}

object ExampleCountWindowAverage {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromCollection(
      List(
        (1L, 3L),
        (1L, 5L),
        (1L, 7L),
        (1L, 4L),
        (2L, 3L),
        (2L, 5L),
        (2L, 7L)
      )
    ).keyBy(_._1)
        .flatMap(new CountWindowAverage)
        .print()

    env.execute("")
  }
}
