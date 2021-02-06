package com.benjamin.broadcast

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2021-02-06
  * Time: 18:57
  * Description:
  */
object TestBroadcastVariables {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    testBroadcastVariables(env)
  }

  def testBroadcastVariables(env: ExecutionEnvironment): Unit ={
    val ds = env.fromElements(0, 1, 2, 3)
    val broadcastDS = env
      .readTextFile("E:/GitProject/flinkshare/flinkshare/src/main/resources/log1.txt")
      .map(s => {
        val arr = s.split(",")
        User(arr(0), arr(1))
      })
    broadcastDS.print()
    ds.map(new MyMapFunction).withBroadcastSet(broadcastDS, "names")
      .print()
  }
}

class MyMapFunction extends RichMapFunction[Int, String] {
  override def map(in: Int): String = {
    val names: util.List[User] = getRuntimeContext.getBroadcastVariable[User]("names")
    names.get(in).name
  }
}
