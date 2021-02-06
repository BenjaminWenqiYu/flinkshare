package com.benjamin.sideoutput

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2020-04-02
  * Time: 11:58 
  * Description:
  */
object SideOutput {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tag = OutputTag[Int]("side_output1")
    val tag2 = OutputTag[String]("side_output2")

    val ds = env.fromCollection(0 until 20).process(new MyProcessFunction(tag, tag2))

    val sideDs1: DataStream[Int] = ds.getSideOutput[Int](tag)
    sideDs1.print("Side Output1 ------>")
    val sideDs2: DataStream[String] = ds.getSideOutput[String](tag2)
    sideDs2.print("Side Output2 ------>")
    ds.print("Normal Output ------>")

    env.execute("")
  }

  class MyProcessFunction(tag: OutputTag[Int], tag2: OutputTag[String]) extends ProcessFunction[Int, Int] {
    override def processElement(i: Int, context: ProcessFunction[Int, Int]#Context,
                                collector: Collector[Int]): Unit = {
      if (i % 5 != 0) {
        context.output(tag, i)
      } else if (i % 5 == 0) {
        context.output(tag2, s"$i is divided by 5")
      }
      if (i % 2 == 0) {
        collector.collect(i)
      }
    }
  }
}
