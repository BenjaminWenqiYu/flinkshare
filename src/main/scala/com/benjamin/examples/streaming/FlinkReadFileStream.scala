package com.benjamin.examples.streaming

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2019-08-05
  * Time: 10:28 
  * Description: 从文件中读取数据  将两个输入流union
  * 按照姓名,性别分组 使用滑动窗口 聚合 ,输出Tuple(String,String,Int)
  */
object FlinkReadFileStream {
  def main(args: Array[String]): Unit = {
    val filePath1 = "resources/log1.txt"
    val filePath2 = "resources/log2.txt"
    import org.apache.flink.api.scala._

    val windowTime = 5

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 将两个文件两个流联合成一个流
    val unionStream = if (null != filePath1 && null != filePath2) {
      val firstStream = env.readTextFile(filePath1)
      val secondStream = env.readTextFile(filePath2)
      firstStream.union(secondStream)
    } else {
      println("ERROR: filePath maybe null")
      null
    }

    // 直接调用执行器
    unionStream.map(x => getRecord(x))
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[UserRecord] {
        override def checkAndGetNextWatermark(lastElement: UserRecord, extractedTimestamp: Long): Watermark = {
          new Watermark(System.currentTimeMillis() - 60)
        }

        override def extractTimestamp(element: UserRecord, previousElementTimestamp: Long): Long = {
          System.currentTimeMillis()
        }
      })
      .filter(_.name != "GuoYijun")
    // 分组 keyBy(0) 指定整个tuple作为key
      .keyBy("name", "sexy")
    // 窗口 使用滚动窗口，滑动窗口
      .window(TumblingEventTimeWindows.of(Time.seconds(windowTime.toInt)))
      .reduce((e1, e2) => UserRecord(e1.name, e1.sexy, e1.shoppingTime + e2.shoppingTime))
      .print()

    env.execute("FlinkReadFileStream")

  }



  def getRecord(line: String): UserRecord = {
    val elems = line.split(",")
    assert(elems.length == 3)
    val name = elems(0)
    val sexy = elems(1)
    val time = elems(2).toInt
    UserRecord(name, sexy, time)
  }

  // 内置pojo类
  case class UserRecord(name: String, sexy: String, shoppingTime: Int)

  private class Record2TimestampExtractor extends AssignerWithPunctuatedWatermarks[UserRecord] {
    override def checkAndGetNextWatermark(lastElement: UserRecord, extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp - 1)
    }

    override def extractTimestamp(element: UserRecord, previousElementTimestamp: Long): Long = {
      System.currentTimeMillis()
    }
  }
}
