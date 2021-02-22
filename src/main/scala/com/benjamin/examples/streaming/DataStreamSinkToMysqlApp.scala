package com.benjamin.examples.streaming

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2019-07-22
  * Time: 14:55 
  * Description:
  */
object DataStreamSinkToMysqlApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    import org.apache.flink.api.scala._
    val personStream = text.map(new MapFunction[String, Person] {
      override def map(value: String): Person = {
        val split = value.split(",")
        Person(Integer.parseInt(split(0)), split(1), Integer.parseInt(split(2)))
      }
    })
    personStream.addSink(new CustomSinkToMysql)

    env.execute("DataStreamSinkToMysqlApp")
  }
}

class CustomSinkToMysql extends RichSinkFunction[Person] {
  private[streaming] var connection: Connection = _
  private[streaming] var pstmt: PreparedStatement = _

  /**
    * 获取数据库连接
    */
  def getConnection(): Connection = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/flink_demo?user=root&password=1234")
  }

  /**
    * 在open方法中建立connection
    * @param parameters
    */
  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    connection = getConnection()
    val sql = "insert into student(id, name, age) values (?, ?, ?)"
    pstmt = connection.prepareStatement(sql)
    System.out.print("open")
  }

  @throws[Exception]
  override def invoke(value: Person, context: SinkFunction.Context[_]): Unit = {
    System.out.print("invoke~~~~~~~~~")
    // 为前面的占位符赋值
    pstmt.setInt(1, value.id)
    pstmt.setString(2, value.name)
    pstmt.setInt(3, value.age)
    pstmt.executeUpdate()
  }

  @throws[Exception]
  override def close(): Unit = {
    super.close()
    if (pstmt != null) pstmt.close()
    if (connection != null) connection.close()
  }
}
