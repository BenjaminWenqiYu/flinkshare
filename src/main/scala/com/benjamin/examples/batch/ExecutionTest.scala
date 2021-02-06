package com.benjamin.examples.batch

import scala.collection.mutable
import scala.collection.immutable.{Queue, Stack}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import com.benjamin.examples.streaming.Person
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

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

  /**
    * 2、基于文件的sink
    * * flink支持多种存储设备上的文件，包括本地文件，hdfs文件等。
    * * flink支持多种文件的存储格式，包括text文件，CSV文件等。
    * *  writeAsText()：TextOuputFormat - 将元素作为字符串写入行。字符串是通过调用每个元素的toString()方法获得的。
    *
    * @param env
    */
  def sinkTest(env: ExecutionEnvironment): Unit ={
    /**
      * 1. 将数据写入本地文件
      * 注意：不论是本地还是hdfs.若Parallelism>1将把path当成目录名称，若Parallelism=1将把path当成文件名。
      * 写入到本地，文本文档
      * NO_OVERWRITE模式下如果文件已经存在，则报错
      * OVERWRITE模式下如果文件已经存在，则覆盖
      */
    val ds1: DataSet[Map[Int, String]] = env.fromElements(Map(1 -> "spark", 2 -> "flink"))
    //    ds1.setParallelism(1).writeAsText("D://resource/a", WriteMode.OVERWRITE)
    //    env.execute()

    /**
      * 2. 将数据写入HDFS
      * 可以使用sortPartition对数据进行排序后再sink到外部系统
      */
    //    ds1.setParallelism(1).writeAsText("hdfs://node3.product.com/SZFK/resultlc/lc/a", WriteMode.OVERWRITE)
    //    env.execute()

    val stu: DataSet[(Int, String, Double)] = env.fromElements(
      (19, "zhangsan", 178.8),
      (17, "lisi", 168.8),
      (18, "wangwu", 184.8),
      (21, "zhaoliu", 164.8)
    )
    //1.以age从小到大升序排列(0->9)
    stu.sortPartition(0, Order.ASCENDING).print
    //2.以name从大到小降序排列(z->a)
    stu.sortPartition(1, Order.ASCENDING).print
    //3.以age升序，height降序排列
    stu.sortPartition(0, Order.ASCENDING).sortPartition(2, Order.DESCENDING).print
    //4.所有字段升序排列
    stu.sortPartition("_", Order.ASCENDING).print

    /**
      *
      */
    case class Student(name: String, age: Int)
    val ds2: DataSet[(Student, Double)] = env.fromElements(
      (Student("zhangsan", 18), 178.5),
      (Student("lisi", 19), 176.5),
      (Student("wangwu", 17), 168.5)
    )
    val ds3 = ds1.sortPartition("_1.age", Order.ASCENDING).setParallelism(1) // TODO: 有问题，待解决
    //5.2写入到hdfs,文本文档
    ds3.writeAsText("D://resource/student1.txt", WriteMode.OVERWRITE)
    env.execute()
    //5.3写入到hdfs,CSV文档
    ds3.writeAsCsv("D://resource/student1.csv", "\n", "|||",WriteMode.OVERWRITE)
    env.execute()
  }

  /**
    * 1、基于本地集合的source数据集
    * @param env
    */
  def sourceTest1(env: ExecutionEnvironment): Unit ={
    // 0.用element创建DataSet
    val ds0: DataSet[String] = env.fromElements("spark", "flink")
    ds0.print()

    // 1.用Tuple创建DataSet
    val ds1: DataSet[(Int, String)] = env.fromElements((1, "spark"), (2, "flink"))
    ds1.print()

    // 2.用Array创建DataSet
    val ds2: DataSet[String] = env.fromCollection(Array("spark", "flink"))
    ds2.print()

    // 3.用ArrayBuffer创建DataSet
    val ds3: DataSet[String] = env.fromCollection(ArrayBuffer("spark", "flink"))
    ds3.print()

    // 4.用List创建DataSet
    val ds4: DataSet[String] = env.fromCollection(List("spark", "flink"))
    ds4.print()

    // 5.用List创建DataSet
    val ds5: DataSet[String] = env.fromCollection(ListBuffer("spark", "flink"))
    ds5.print()

    // 6.用Vector创建DataSet
    val ds6: DataSet[String] = env.fromCollection(Vector("spark", "flink"))
    ds6.print()

    // 7.用Queue创建DataSet
    val ds7: DataSet[String] = env.fromCollection(Queue("spark", "flink"))
    ds7.print()

    // 8.用Stack创建DataSet
    val ds8: DataSet[String] = env.fromCollection(Stack("spark", "flink"))
    ds8.print()

    // 9.用Stream创建DataSet（Stream相当于lazy List，避免在中间过程中生成不必要的集合）
    val ds9: DataSet[String] = env.fromCollection(Stream("spark", "flink"))
    ds9.print()

    // 10.用Seq创建DataSet
    val ds10: DataSet[String] = env.fromCollection(Seq("spark", "flink"))
    ds10.print()

    // 11.用Set创建DataSet
    val ds11: DataSet[String] = env.fromCollection(Set("spark", "flink"))
    ds11.print()

    // 12.用Iterable创建DataSet
    val ds12: DataSet[String] = env.fromCollection(Iterable("spark", "flink"))
    ds12.print()

    // 13.用ArraySeq创建DataSet
    val ds13: DataSet[String] = env.fromCollection(mutable.ArraySeq("spark", "flink"))
    ds13.print()

    // 14.用ArrayStack创建DataSet
    val ds14: DataSet[String] = env.fromCollection(mutable.ArrayStack("spark", "flink"))
    ds14.print()

    // 15.用Map创建DataSet
    val ds15: DataSet[(Int, String)] = env.fromCollection(Map(1 -> "spark", 2 -> "flink"))
    ds15.print()

    // 16.用Range创建DataSet
    val ds16: DataSet[Int] = env.fromCollection(Range(1, 9))
    ds16.print()

    // 17.用generateSequence创建DataSet
    val ds17: DataSet[Long] = env.generateSequence(1, 9)
    ds17.print()
  }

  /**
    * 2、基于文件的source
    * @param env
    */
  def sourceTest2(env: ExecutionEnvironment): Unit ={
    val datas: DataSet[String] = env.readTextFile("D://resource/people.txt")

    /**
      * 1.读取本地文件 -- 加载数据 -- 指定数据的转化
      *
      */
    val res1 = datas.flatMap(line => line.split("\\W+"))
      .map(line => (line, 1))
      .groupBy(_._1).reduce((x, y) => (x._1, x._2 + y._2))
    //    res1.print()

    /**
      * 2、读取hdfs数据
      */
    val file = env.readTextFile("hdfs://node3.product.com/SZFK/resultlc/lc/dlgz/2018-11-01/SCHEMA")
    val res2 = file.flatMap(line => line.split("\\W+")).map(line => (line, 1))
      .groupBy(_._1).reduce((x, y) => (x._1, x._2 + y._2))
    //    res2.print()

    /**
      * 3、读取csv数据
      */
    val csvFile: DataSet[People] = env
      .readCsvFile[(String, Int, String)]("D://resource/people.csv",
      fieldDelimiter = ";",ignoreFirstLine = true)
      .map(p => People(p._1, p._2, p._3))
    //    csvFile.print()

    /**
      * 4、基于文件的source（遍历目录）
      *
      * flink支持对一个文件目录内的所有文件，包括所有子目录中的所有文件的遍历访问方式。
      * 对于从文件中读取数据，当读取的数个文件夹的时候，嵌套的文件默认是不会被读取的，只会读取第一个文件，其他的都会被忽略。
      * 所以我们需要使用recursive.file.enumeration进行递归读取
      */
    val parameters = new Configuration()
    parameters.setBoolean("recursive.file.enumeration", true) // 开启递归
    val res3 = env.readTextFile("D:\\resource").withParameters(parameters)
    //    res3.print()

    /**
      * 5、读取压缩文件
      *
      * 对于以下压缩类型，不需要指定任何额外的inputformat方法，flink可以自动识别并且解压。
      * 但是，压缩文件可能不会并行读取，可能是顺序读取的，这样可能会影响作业的可伸缩性。
      */
    env.readTextFile("test/data1/zookeeper.out.gz").print()
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
case class People(name: String, age: Int, job: String)
