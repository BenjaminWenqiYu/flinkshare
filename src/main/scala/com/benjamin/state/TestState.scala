package com.benjamin.state

import java.util

import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2021-02-06
  * Time: 19:00
  * Description: 这些都是针对Keyed Stream进行的操作
  */
object TestState1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromCollection(0 until 10).keyBy(x => x % 2)

    //    testValueState(ds).print("Value State---------->")

    //    testListState(ds).print("List State ------->")

    //    testReducingState(ds).print("Reducing State ------>")

    testMapState(ds).print("Map State ------>")

    env.execute()
  }

  def testMapState(ds: DataStream[Int]): DataStream[(Int, Long)] = {
    ds.process(new MapStateRichFunction)
  }

  def testReducingState(ds: DataStream[Int]): DataStream[(Int, Long)] = {
    ds.flatMap(new ReducingStateRichFunction)
  }

  def testListState(ds: DataStream[Int]): DataStream[(Int, java.util.List[Int])] = {
    ds.flatMap(new ListStateRichFuntion)
  }

  def testValueState(ds: DataStream[Int]): DataStream[(Int, Long)] = {
    ds.flatMap(new ValueStateRichFunction)
  }

  class MapStateRichFunction extends ProcessFunction[Int, (Int, Long)] {
    private var mapState: MapState[Int, Long] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      mapState = getRuntimeContext.getMapState(new MapStateDescriptor[Int, Long]("map_state",
        TypeInformation.of(classOf[Int]), TypeInformation.of(classOf[Long])))
    }

    override def processElement(i: Int, context: ProcessFunction[Int, (Int, Long)]#Context,
                                collector: Collector[(Int, Long)]): Unit = {
      val key = getRuntimeContext.getIndexOfThisSubtask
      val newSum = if (mapState.contains(key)) {
        mapState.get(key) + i
      } else {
        0L
      }
      mapState.put(key, newSum)
      collector.collect((key, newSum))
    }
  }

  class ReducingStateRichFunction extends RichFlatMapFunction[Int, (Int, Long)] {
    private var state: ReducingState[Long] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 此处传入的为ReducingFunction
      state = getRuntimeContext.getReducingState(new ReducingStateDescriptor("reducing-descriptor",
        new ReduceFunction[Long] {
          override def reduce(t: Long, t1: Long): Long = {
            t + t1
          }
        }, TypeInformation.of(classOf[Long])))
    }

    override def flatMap(in: Int, collector: Collector[(Int, Long)]): Unit = {
      state.add(in)
      collector.collect((in, state.get()))
    }
  }

  /**
    * 对每一个元素，按照key进行统计
    */
  class ListStateRichFuntion extends RichFlatMapFunction[Int, (Int, java.util.List[Int])] {
    private var list: ListState[Int] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 获取每一个key的上一个状态
      list = getRuntimeContext.getListState(new ListStateDescriptor[Int]("list-descriptor",
        TypeInformation.of(classOf[Int])))
    }

    override def flatMap(in: Int, collector: Collector[(Int, util.List[Int])]): Unit = {
      list.add(in)
      import scala.collection.JavaConverters._
      val res: util.List[Int] = list.get().asScala.toList.asJava
      collector.collect((in, res))
    }
  }

  /**
    * 对每一个元素，按照key求和
    */
  class ValueStateRichFunction extends RichFlatMapFunction[Int, (Int, Long)] with CheckpointedFunction {
    private var sum: ValueState[Long] = _
    // 获取每一个key的上一个状态
    private val valueState: ValueStateDescriptor[Long] =
      new ValueStateDescriptor("sum-descriptor", TypeInformation.of(classOf[Long]))

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      sum = getRuntimeContext.getState(valueState)
    }

    override def flatMap(in: Int, collector: Collector[(Int, Long)]): Unit = {
      val newSum = sum.value() + in
      sum.update(newSum)
      collector.collect(in, newSum)
    }

    override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = ???

    override def initializeState(context: FunctionInitializationContext): Unit = {
      sum = context.getKeyedStateStore
        .getState(valueState)
    }
  }
}
