package com.atguigu.kemai

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

case class InputData(tableName:String,jsonObj:JSONObject)

object TestHandleKemai {
  System.setProperty("HADOOP_USER_NAME","root")
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("TestHandleKemai")
      // TODO: 访问hdfs namenode高可用集群，设置0.0.0.0:9820，无法使用,提示没有权限！
//      .config("fs.defaultFS", "hdfs://jtb:9820")
//      .config("dfs.nameservices", "jtb")
//      .config("dfs.ha.namenodes.jtb", "nn1,nn2")
//      .config("dfs.namenode.rpc-address.jtb.nn1", "node11:9820")
//      .config("dfs.namenode.rpc-address.jtb.nn2", "node12:9820")
//      .config("dfs.client.failover.proxy.provider.jtb", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
      .getOrCreate()

    val sc = spark.sparkContext

    val path_prefix = "hdfs://hadoop102:9820/transform/"   //jtb/61.132.230.81:8020

    val inputRDD: RDD[String] = sc.textFile(path_prefix + "ent_zhaodao/*")
    inputRDD.collect().foreach(println(_))


//    // TODO: 也可以用直接父级目录
////    val path = "hdfs://hadoop102:9820/transform/ent/2020-12-04"
//    import spark.implicits._
//    // TODO: path一定要到分叉的那一层目录，否则就不不行！
//    val inputDF: DataFrame = spark.read.json(
//			path_prefix + "ent_zhaodao/*"
////      			path_prefix + "ent/*", // ES索引，统计字段
////      			path_prefix + "ent_a_taxpayer/*", // ES索引，统计字段
////      			path_prefix + "ent_abnormal_opt/*", // ES索引
////      			path_prefix + "ent_annual_report/*", // 统计字段
////      			path_prefix + "ent_apps/*", // ES索引，统计字段
////      			path_prefix + "ent_bids/*", // ES索引
////      			path_prefix + "ent_brand/*", // ES索引
////      			path_prefix + "ent_cert/*", // ES索引，统计字段
////      			path_prefix + "ent_contacts/*", // ES索引，统计字段
////      			path_prefix + "ent_copyrights/*", // ES索引
////      			path_prefix + "ent_court_notice/*", // 统计字段
////      			path_prefix + "ent_court_operator/*", // ES索引
////      			path_prefix + "ent_court_paper/*", // 统计字段
////      			path_prefix + "ent_dishonesty_operator/*", //
////      			path_prefix + "ent_ecommerce/*", // ES索引，统计字段
////      			path_prefix + "ent_equity_pledged/*", // 统计字段
////      			path_prefix + "ent_funding_event/*", // ES索引
////      			path_prefix + "ent_goods/*", // ES索引
////      			path_prefix + "ent_invest_company/*", // 统计字段
////      			path_prefix + "ent_licence/*", // ES索引，统计字段
////      			path_prefix + "ent_new_media/*", // ES索引，统计字段
////      			path_prefix + "ent_news/*", // ES索引，统计字段
////      			path_prefix + "ent_patent/*", // ES索引，统计字段
////      			path_prefix + "ent_punishment/*", // ES索引，统计字段
////      			path_prefix + "ent_recruit/*", // ES索引，统计字段
////      			path_prefix + "ent_software/*", // ES索引，统计字段
////      			path_prefix + "ent_trademark/*", // ES索引，统计字段
////      			path_prefix + "ent_website/*", // ES索引，统计字段
////      			path_prefix + "ent_maimai/*", //
////      			path_prefix + "ent_zhaodao/*" //
//    ).cache()
//
//		val resultDF2 = inputDF.rdd
////			.map(row =>{
////				//        import scala.collection.JavaConversions._
////					println("===================="+row.toString())
////					val strings: Array[String] = row.toString().split("\\|")
////					val tableName = strings(0).substring(1)
////					val jsonStr = strings(1).substring(0, strings(1).length - 1)
////					val jsonObj: JSONObject = JSON.parseObject(jsonStr)
////					//        InputData(tableName,jsonObj)
////				(tableName,jsonStr)
////			})
//      .mapPartitions(iter =>{
////        import scala.collection.JavaConversions._
//        val arrayBuffer = ArrayBuffer[InputData]()
//        for(row <- iter){
//          println("===================="+row.toString())
//					val str = row.toString()
//					val tableName = str.substring(1,str.indexOf("|"))//去掉第一个[
//					val jsonStr = str.substring(str.indexOf("|")+1,str.length-1)//去掉最后一个]
////					println(tableName+" "+jsonStr)
//					val jsonObj: JSONObject = JSON.parseObject(jsonStr)
////					try{
////
////					}catch{
////						case e:Exception => {
////							e.printStackTrace()
////							println("****************=:"+row.toString())
////						}
////					}
//          //        InputData(tableName,jsonObj)
////					arrayBuffer.append((tableName,jsonStr))
//					arrayBuffer.append(InputData(tableName,jsonObj))
//        }
//				arrayBuffer.iterator
//      })
//    .toDF("tableName","jsonStr")
//
//		resultDF2.show(truncate = false)
//    println(resultDF2.rdd.getNumPartitions)


//    val path_prefix = "D:\\JavaRelation\\工作\\安徽创瑞\\mongoDatas\\transform\\"
    //path不支持传string，用逗号分隔，而textFile支持
//    val inputDF = spark.read.format("json")
//      .load(
////        path_prefix + "ent\\*",
//        path_prefix + "ent_top500\\*"
//      )
//      .rdd
//      .map(row=>{
////        println(row.toString())
//        val strings: Array[String] = row.toString().split("\\|")
//        val tableName = strings(0).substring(1)
//        val jsonStr = strings(1).substring(0, strings(1).length - 1)
//        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
////        InputData(tableName,jsonObj)
//        (tableName,jsonStr)
//      })
//      .toDF("tableName","jsonStr")
//
//    println("=======================================================================")
//    inputDF.show(truncate = false)
////    inputDF.collect.foreach(println(_))
//    println("=======================================================================")




    spark.stop()
  }

}
