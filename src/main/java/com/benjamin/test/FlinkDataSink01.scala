package com.benjamin.test

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2019-06-14
  * Time: 10:48 
  * Description: 1、基于本地集合的sink（Collection-based-sink）
  */
object FlinkDataSink01 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 1.定义数据
    val stu: DataSet[(Int, String, Double)] = env.fromElements(
      (19, "zhangsan", 178.8),
      (17, "lisi", 168.8),
      (18, "wangwu", 184.8),
      (21, "zhaoliu", 164.8)
    )

    // 2.sink到标准输出
    stu.print()
    // 3.sink到标准error输出
    stu.printToErr()
    // 4.sink到本地Collection
    println(stu.collect())
  }
}
