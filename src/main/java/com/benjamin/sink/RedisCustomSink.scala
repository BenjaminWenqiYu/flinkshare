package com.benjamin.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.StringUtils
import redis.clients.jedis.Jedis


/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2019-12-17
  * Time: 09:21 
  * Description:
  */
object RedisCustomSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val text = env.socketTextStream("localhost", 6379)

    val data = text.filter(!StringUtils.isNullOrWhitespaceOnly(_))
      .flatMap(_.split(","))
      .map(MySaveInfo("RedisCustomSink", _))

    data.addSink(new CustomSinkToRedis())

    env.execute("sink_demo")
  }
}

class CustomSinkToRedis extends RichSinkFunction[MySaveInfo] {
  var redisCon: Jedis = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    this.redisCon = new Jedis("localhost", 6379)
  }

  override def close(): Unit = {
    super.close()
    if (null != this.redisCon) {
      this.redisCon.close()
    }
  }

  override def invoke(value: MySaveInfo, context: SinkFunction.Context[_]): Unit = {
    //super.invoke(value, context)
    println(value)
    println("sadd之前" + this.redisCon.ttl(value.key))
    this.redisCon.sadd(value.key, value.value)
    println("sadd之后" + this.redisCon.ttl(value.key))
    if (this.redisCon.ttl(value.key) == -1) {
      this.redisCon.expire(value.key, 60 * 60)
    }
    println("设置过期之后/每次更新后" + this.redisCon.ttl(value.key))
  }
}

case class MySaveInfo(key: String, value: String)
