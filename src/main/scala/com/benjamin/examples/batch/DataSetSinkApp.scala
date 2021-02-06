package com.benjamin.examples.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2019-07-22
  * Time: 16:17 
  * Description:
  */
object DataSetSinkApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val data = 1 to 10
    val text = env.fromCollection(data)

    // 可以写入任何文件系统
    text.writeAsText("", WriteMode.OVERWRITE)

    env.execute("SinkApp")
  }
}
