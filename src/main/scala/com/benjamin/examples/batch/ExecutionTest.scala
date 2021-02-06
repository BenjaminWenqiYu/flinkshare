package com.benjamin.examples.batch

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2021-02-02
  * Time: 10:59
  * Description:
  */
object ExecutionTest {
  def main(args: Array[String]): Unit = {
    // 获取批处理运行时
    val env = ExecutionEnvironment.getExecutionEnvironment
    sortFunction(env)
  }

  def sortFunction(env: ExecutionEnvironment): Unit = {
    // 添加Source
    val text = env.fromElements(
      "Who's there?",
      "I think I hear them. Stand, ho! Who's there?"
    )
    // "\\W+" 匹配非字母、非数字、非下划线   "\\w+" 匹配字母、数字、下划线
    val counts = text.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }
      .filter(_.startsWith("s")).map(w => (Word(w, w.length), 1))
      .sortPartition("_1.name", Order.ASCENDING)
    // 定义Sink
    counts.print()
  }
}

case class Word(name: String, count: Int)
