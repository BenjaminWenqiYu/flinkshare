package com.benjamin.examples.basic

import java.net.InetAddress

/**
  * Created with IntelliJ IDEA.
  * User: ywq
  * Date: 2021-02-01
  * Time: 17:20
  * Description:
  */
object InetAddressTest {
  def main(args: Array[String]): Unit = {
    getInetAddress()
  }

  def getInetAddress(): Unit = {
    // 使用getLocalHost方法为InetAddress创建对象
    val add = InetAddress.getLocalHost; // 获得本机的InetAddress对象
    println(add.getHostAddress)// 返回本机IP地址
    println(add.getHostName) // 输出计算机名

    val address = InetAddress.getByName("www.baidu.com")
    println(s"百度服务器的IP地址：${address.getHostAddress}")
    println(s"百度服务器的主机名：${address.getHostName}")
  }
}
