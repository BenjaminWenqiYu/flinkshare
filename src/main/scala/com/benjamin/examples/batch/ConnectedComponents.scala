package com.benjamin.examples.batch

import java.lang.Iterable

import org.apache.flink.api.common.functions.{GroupCombineFunction, GroupReduceFunction, RichMapFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.operators.base.JoinOperatorBase
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2019-06-12
  * Time: 17:03
  * Description:

  *
  */
object ConnectedComponents {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[String] = env.fromElements("A;B;C;D;B;D;C;B;D;A;E;D;C;A;B")
//    sumNearCode(env, data)

    val data2: DataSet[List[(String, Int)]] = env.fromElements(List(("java", 1), ("scala", 1), ("java", 1)))
//    useReduce(data2)
//    useReduceGroup(data2)
//    useUserDefinedReduceGroup(data2)

    val data3 = createData(env)
//    useAggregate(data3)
//    useMinByAndMaxBy(data3)
//    useDistinct(data3)
//    useJoin(createData2(env), createData3(env))
//    useUnion(env)
//    useRebalance(env)
//    usePartition(env)
//    useFirst(env)

  }

  /**
    * 统计相邻字符串出现的次数(A+B , 2) (B+C , 1)…
    *             A;B;C;D;B;D;C
    *             B;D;A;E;D;C
    *             A;B
    * @param env
    * @param data
    */
  def sumNearCode(env: ExecutionEnvironment, data: DataSet[String]): Unit = {
    val map_data: DataSet[Array[String]] = data.map(line => line.split(";"))
    val tuple_data = map_data.flatMap { line =>
      for (index <- 0 until line.length - 1) yield (line(index) + "+" + line(index + 1), 1)
    }
    val group_data = tuple_data.groupBy(0)
    val result: AggregateDataSet[(String, Int)] = group_data.sum(1)
    result.print()
  }

  /**
    * 使用reduce对数据进行聚合
    * @param data
    */
  def useReduce(data: DataSet[List[(String, Int)]]): Unit = {
    val tuple_data: DataSet[(String, Int)] = data.flatMap(x => x)
    val group_data: GroupedDataSet[(String, Int)] = tuple_data.groupBy(x => x._1)
    val reduce = group_data.reduce((x, y) => (x._1, x._2 + y._2))
    reduce.print()
  }

  /**
    * reduceGroup是reduce的一种优化方案
    *     会先分组reduce，然后再做整体的reduce，这样做的好处就是可以减少网络IO
    * @param data
    */
  def useReduceGroup(data: DataSet[List[(String, Int)]]): Unit = {
    data.flatMap(x => x)
      .groupBy(_._1)
      .reduceGroup {(in: Iterator[(String, Int)], out: Collector[(String, Int)]) =>
        val result = in.reduce((x, y) => (x._1, x._2 + y._2))
        out.collect(result)
      }
      .print()
  }

  /**
    * 使用自定义的reduceGroup方法
    * @param data
    */
  def useUserDefinedReduceGroup(data: DataSet[List[(String, Int)]]): Unit = {
    val res = data.flatMap(line => line)
      .groupBy(_._1)
      .reduceGroup(new Tuple3GroupReduceWithCombine())
      .collect().sortBy(_._1)
    println(res)
  }

  /**
    * 在数据集上进行聚合求最值（最大值、最小值）
    *     Aggregate只能作用于元组上
    */
  def useAggregate(data: DataSet[(Int, String, Double)]): Unit = {
    data.groupBy(1).aggregate(Aggregations.MAX, 2).print()
  }

  /**
    * 求每个学科下的最小或者最大分数
    *     minBy的参数代表求哪个字段的最小值
    *     maxBy的参数代表求哪个字段的最大值
    * @param data
    */
  def useMinByAndMaxBy(data: DataSet[(Int, String, Double)]): Unit = {
    data.groupBy(1)
      .minBy(2)
      .print()

    data.groupBy(1)
      .maxBy(2)
      .print()
  }

  /**
    * 使用distinct去重
    * @param data
    */
  def useDistinct(data: DataSet[(Int, String, Double)]): Unit = {
    data.distinct(1).print()
  }

  /**
    * Flink在操作过程中，需要关联组合操作，这样可以方便返回想要的关联结果
    *   求每个班级的每个学科的最高分数
    * @param data1
    * @param data2
    */
  def useJoin(data1: DataSet[(Int, String, Double)], data2: DataSet[(Int, String)]): Unit = {
    // 求每个班级下每个学科最高分数
    val joinData = data1.join(data2, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND)
      .where(0).equalTo(0) {
      (input1, input2) => (input1._1, input1._2, input2._2, input1._3)
    }
    joinData.print()
    println("按照学科和班级的最高分")
    joinData.groupBy(1, 2).aggregate(Aggregations.MAX, 3).print()
  }

  /**
    *   将多个DataSet合并成一个DataSet
    *
    *       【注意】：union合并的DataSet的类型必须是一致的
    * @param env
    */
  def useUnion(env: ExecutionEnvironment): Unit = {
    val data1: DataSet[String] = env.fromElements(("123"))
    val data2 = env.fromElements(("456"))
    val data3 = env.fromElements(("123"))
    data1.union(data2).union(data3).print()
  }

  /**
    * Flink也有数据倾斜的时候，比如当前有数据量大概10亿条数据需要处理，在处理过程中可能会发生如图所示的状况：
    *     这个时候本来总体数据量只需要10分钟解决的问题，出现了数据倾斜，机器1上的任务需要4个小时才能完成，那么其他3台机器执行完毕
    *     也要等待机器1执行完毕后才算整体将任务完成；
    *
    *     所以在实际的工作中，出现这种情况比较好的解决方案就是本节课要讲解的
    *         rebalance（内部使用round robin方法将数据均匀打散。这对于数据倾斜时是很好的选择。）
    *
    *         每隔8一次循环（数据使用轮询的方式在各个子任务中执行）
    * @param env
    */
  def useRebalance(env: ExecutionEnvironment): Unit = {
    // 1.不使用rebalance的情况下，观察每一个线程执行的任务特点
    val ds = env.generateSequence(1, 3000)
    val rebalanced = ds.filter(_ > 780)
    val countsInPartition = rebalanced.map(new RichMapFunction[Long, (Int, Long)] {
      override def map(in: Long): (Int, Long) = {
        // 获取并行时子任务的编号getRuntimeContext.getIndexOfThisSubtask
        (getRuntimeContext.getIndexOfThisSubtask, in)
      }
    })
    countsInPartition.print()

    // 2.数据随机的分发给各个子任务（分区） -- 使用rebalance
    val skewed = ds.filter(_ > 780)
    val rebalance = skewed.rebalance()
    rebalance.map(new RichMapFunction[Long, (Int, Long)] {
      override def map(in: Long): (Int, Long) = {
        (getRuntimeContext.getIndexOfThisSubtask, in)
      }
    }).print()
  }

  /**
    * 分区的使用
    */
  def usePartition(env: ExecutionEnvironment): Unit = {
    // 1、使用partitionByHash
    val data: DataSet[(Int, Long, String)] = createData5(env)
    val unique: DataSet[(Int, Long, String)] = data.partitionByHash(1)
      .mapPartition{ line => line.map(x => (x._1, x._2, x._3))}
    unique.writeAsText("G:\\union", WriteMode.NO_OVERWRITE)
    env.execute()

    // 2、使用RangePartition
    data.partitionByRange(x => x._1).mapPartition(line => line.map(x => (x._1, x._2, x._3)))
      .writeAsText("G:\\union2", WriteMode.NO_OVERWRITE)
    env.execute()

    // 3、使用sortPartition -- 根据指定的字段值进行分区的排序
    val res = data.map(x => x).setParallelism(2)
      .sortPartition(1, Order.DESCENDING) // 第一个参数代表按照哪个字段进行分区  第二个参数代表排序方式
      .mapPartition(line => line)
      .collect()
    println(res)
  }

  /**
    * 取前N个值
    * @param env
    */
  def useFirst(env: ExecutionEnvironment): Unit = {
    val data = createData5(env)
    data.first(10).print()
    data.groupBy(_._2).first(2).print()
  }

  /**
    * 自定义创建数据的方法
    * @param env
    * @return
    */
  def createData(env: ExecutionEnvironment): DataSet[(Int, String, Double)] = {
    val data = new mutable.MutableList[(Int, String, Double)]
    data.+= ((1, "Chinese", 89.0))
    data.+= ((2, "Math", 92.2))
    data.+= ((3, "English", 89.99))
    data.+= ((4, "Physics", 98.9))
    data.+= ((1, "Chinese", 88.88))
    data.+= ((1, "Physics", 93.00))
    data.+= ((1, "Chinese", 94.3))
    val input: DataSet[(Int, String, Double)] = env.fromCollection(Random.shuffle(data))
    input
  }

  /**
    * 学生学号 -- 学科 -- 分数
    * @param env
    * @return
    */
  def createData2(env: ExecutionEnvironment): DataSet[(Int, String, Double)] = {
    val data = new mutable.MutableList[(Int, String, Double)]
    data.+= ((1, "Chinese", 90.0))
    data.+= ((1, "Math", 90.0))
    data.+= ((2, "English",  20.0))
    data.+= ((3, "Chinese",  30.0))
    data.+= ((4, "Math", 40.0))
    data.+= ((5, "English",  50.0))
    data.+= ((6, "Chinese",  60.0))
    data.+= ((7, "English", 70.0))
    data.+= ((8, "Chinese", 20.0))
    env.fromCollection(data)
  }

  /**
    * 学号 -- 班级
    * @param env
    * @return
    */
  def createData3(env: ExecutionEnvironment): DataSet[(Int, String)] = {
    val data = new mutable.MutableList[(Int, String)]
    data.+= ((1, "class_1"))
    data.+= ((2, "class_1"))
    data.+= ((3, "class_2"))
    data.+= ((4, "class_2"))
    data.+= ((5, "class_3"))
    data.+= ((6, "class_3"))
    data.+= ((7, "class_4"))
    data.+= ((8, "class_1"))
    env.fromCollection(data)
  }

  def createData5(env: ExecutionEnvironment): DataSet[(Int, Long, String)] = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+= ((1, 1L, "Hi"))
    data.+= ((2, 2L, "Hello"))
    data.+= ((3, 2L, "Hello world"))
    data.+= ((4, 3L, "Hello world, how are you?"))
    data.+= ((5, 3L, "I am fine."))
    data.+= ((6, 3L, "Luke Skywalker"))
    data.+= ((7, 4L, "Comment#1"))
    data.+= ((8, 4L, "Comment#2"))
    data.+= ((9, 4L, "Comment#3"))
    data.+= ((10, 4L, "Comment#4"))
    data.+= ((11, 5L, "Comment#5"))
    data.+= ((12, 5L, "Comment#6"))
    data.+= ((13, 5L, "Comment#7"))
    data.+= ((14, 5L, "Comment#8"))
    data.+= ((15, 5L, "Comment#9"))
    data.+= ((16, 6L, "Comment#10"))
    data.+= ((17, 6L, "Comment#11"))
    data.+= ((18, 6L, "Comment#12"))
    data.+= ((19, 6L, "Comment#13"))
    data.+= ((20, 6L, "Comment#14"))
    data.+= ((21, 6L, "Comment#15"))
    env.fromCollection(Random.shuffle(data))
  }
}

/**
  * 自定义reduce方法
  */
class Tuple3GroupReduceWithCombine extends GroupReduceFunction[(String, Int), (String, Int)] with
  GroupCombineFunction[(String, Int), (String, Int)] {
  override def reduce(values: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
    for (in <- values.asScala) {
      out.collect((in._1, in._2))
    }
  }

  override def combine(values: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
    val map = new mutable.HashMap[String, Int]
    var num = 0
    var s = ""
    for (in <- values.asScala) {
      num += in._2
      s = in._1
    }
    out.collect((s, num))
  }
}

/**
  *   注意：使用combineGroup可能得不到完整的结果而是部分的结果
  *
  *     使用之前的group操作，比如：reduceGroup或者GroupReduceFunction
  *         这种操作很容易造成内存溢出 ———— 因为需要一次性把所有的数据一步转化到位
  *         所以需要足够的内存支撑，如果内存不够的情况下，那么需要使用combineGroup
  *         combineGroup在分组数据集上应用GroupCombineFunction。
  *         GroupCombineFunction类似于GroupReduceFunction，但不执行完整的数据交换
  */
class MyCombineGroup extends GroupCombineFunction[String, (String, Int)] {
  override def combine(values: Iterable[String], out: Collector[(String, Int)]): Unit = {
    var key: String = null
    var count = 0
    for (line <- values.asScala) {
      key = line
      count += 1
    }
    out.collect((key, count))
  }
}