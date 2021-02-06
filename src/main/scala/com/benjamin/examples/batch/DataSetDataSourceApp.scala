package com.benjamin.examples.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2019-07-22
  * Time: 16:07 
  * Description:
  */
object DataSetDataSourceApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

  }

  /**
    * 集合DataSource
    */
  def fromCollection(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val data = 1 to 10
    env.fromCollection(data).print()
  }

  /**
    * 文件/文件夹datasource
    */
  def textFile(env: ExecutionEnvironment): Unit = {
    //可以直接指定文件夹
    env.readTextFile("").print()
  }

  /**
    * csv  datasource
    */
  def csvFile(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val filePath = ""
    //[T]可以指定为tuple或者pojo case class,可以指定需要的列或在参数重指定 includedFields = Array(0,1)列
    //文件路径     是否忽略第一行
    env.readCsvFile[(String, Int, String)](filePath, ignoreFirstLine = true).print()
  }

  def readRecuriseFiles(env: ExecutionEnvironment): Unit = {
    val filePath = ""
    val parameters = new Configuration()

    parameters.setBoolean("recursive.file.enumeration", true)
    env.readTextFile(filePath).withParameters(parameters).print()
  }

  def readCompressionFiles(env: ExecutionEnvironment): Unit = {
    val filePath = ""
    env.readTextFile(filePath).print()
  }
}
