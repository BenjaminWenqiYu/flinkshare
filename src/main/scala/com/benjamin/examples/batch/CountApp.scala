package com.benjamin.examples.batch

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2019-07-22
  * Time: 15:26 
  * Description: flink中计数器的实现
  */
object CountApp {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")

    val info = data.map(new RichMapFunction[String, String]() {
      // 定义计数器
      val counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        // 注册计数器
        getRuntimeContext.addAccumulator("ele-counts-scala", counter)
      }

      override def map(in: String): String = {
        counter.add(1)
        in
      }
    })

//    val filePath = "file:///Users/lipan/workspace/flink_demo/flink-local-train/src/main/resources/sink"
//    info.writeAsText(filePath, WriteMode.OVERWRITE).setParallelism(3)
    info.print()
    val jobResult = env.execute("CounterApp")

    // 获取计数器
    val num = jobResult.getAccumulatorResult[Long]("ele-counts-scala")

    println("num: " + num)
  }
}
