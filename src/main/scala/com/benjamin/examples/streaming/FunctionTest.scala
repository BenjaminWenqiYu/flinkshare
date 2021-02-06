package com.benjamin.examples.streaming

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2021-02-06
  * Time: 18:48
  * Description:
  */
object FunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val dataStream = env.readTextFile("resource/log2.txt")
      .map { r =>
        val arr = r.split(",")
        Human(arr.head, arr(1), arr(2).toLong)
      }

    dataStream.keyBy(h => (h.name, h.sex))
      .timeWindow(Time.milliseconds(100))
      .aggregate(new AverageAggregate, new MyProcessWindowFunction)
      .print()
    env.execute()
  }
}

case class Human(name: String, sex: String, age: Long)

case class Person1(name: String, sex: String, description: String, averageAge: Double)

/**
  * 实时输出均值结果
  */
class MyProcessWindowFunction extends ProcessWindowFunction[Double, Person1, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, averages: Iterable[Double],
                       out: Collector[Person1]): Unit = {
    // 输出当前平均值
    val average = averages.iterator.next()
    out.collect(Person1(key._1, key._2, s"Window ${context.window} key: $key", average))
  }
}

/**
  * 聚合函数增量式计算均值
  */
class AverageAggregate extends AggregateFunction[Human, (Long, Long), Double] {
  // 初始化累加器
  override def createAccumulator(): (Long, Long) = (0L, 0L)
  //累加器的第一个参数为元素类累加和，第二个参数为元素数量
  override def add(in: Human, acc: (Long, Long)): (Long, Long) = {
    (acc._1 + in.age, acc._2 + 1L)
  }
  // 输出平均值
  override def getResult(acc: (Long, Long)): Double = {
    acc._1 / acc._2.toDouble
  }

  override def merge(acc: (Long, Long), acc1: (Long, Long)): (Long, Long) = {
    (acc._1 + acc1._1, acc._2 + acc1._2)
  }
}
