package com.atguigu.kemai.originCode

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by JarvisKwok
 * Date :2020/11/11
 * Description :
 */
object ReadFromEs {
  def main(args: Array[String]): Unit = {

    val options = Map(
      "es.nodes.wan.only" -> "true", // 域名访问
      "es.nodes" -> "61.132.230.81", // 客户端节点
      "es.port" -> "9999" // 端口号
    )

    val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[*]")
    val session = SparkSession.builder().config(conf).getOrCreate()

    val dataFrame = session.read.format("es").options(options).load("ent")
    dataFrame.show()
  }
}
