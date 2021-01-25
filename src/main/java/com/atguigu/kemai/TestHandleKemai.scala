package com.atguigu.kemai

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.kemai.es.{ElasticSearchUtil, EsHandle}
import com.atguigu.kemai.mango.MangoHandle
import com.atguigu.kemai.recommend.{CategoryItem, GetCategory}
import com.atguigu.kemai.utils.JSONUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.util
import scala.collection.mutable.ArrayBuffer

//case class InputData(tableName:String,jsonObj:JSONObject)

/**
 * 全量数据处理
 */
object TestHandleKemai {
  System.setProperty("HADOOP_USER_NAME","root")
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
//      .master("local")
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

//    val path_prefix = "hdfs://hadoop102:9820/transform/"   //jtb/61.132.230.81:8020
    val path_prefix = "hdfs://jtb/transform/2020/11/03/"   //jtb/61.132.230.81:8020
    val fileList = Array(
      path_prefix + "ent/*", // ES索引，统计字段
      path_prefix + "ent_a_taxpayer/*", // ES索引，统计字段
      path_prefix + "ent_abnormal_opt/*", // ES索引
      path_prefix + "ent_annual_report/*", // 统计字段
      path_prefix + "ent_apps/*", // ES索引，统计字段
      path_prefix + "ent_bids/*", // ES索引
      path_prefix + "ent_brand/*", // ES索引
      path_prefix + "ent_cert/*", // ES索引，统计字段
      path_prefix + "ent_contacts/*", // ES索引，统计字段
      path_prefix + "ent_copyrights/*", // ES索引
      path_prefix + "ent_court_notice/*", // 统计字段
      path_prefix + "ent_court_operator/*", // ES索引
      path_prefix + "ent_court_paper/*", // 统计字段
      path_prefix + "ent_dishonesty_operator/*", //
      path_prefix + "ent_ecommerce/*", // ES索引，统计字段
      path_prefix + "ent_equity_pledged/*", // 统计字段
      path_prefix + "ent_funding_event/*", // ES索引
      path_prefix + "ent_goods/*", // ES索引
//      path_prefix + "ent_growingup/*", // 统计字段
      path_prefix + "ent_invest_company/*", // 统计字段
      path_prefix + "ent_licence/*", // ES索引，统计字段
//      path_prefix + "ent_listed/*", // 统计字段
      path_prefix + "ent_new_media/*", // ES索引，统计字段
      path_prefix + "ent_news/*", // ES索引，统计字段
      path_prefix + "ent_patent/*", // ES索引，统计字段
      path_prefix + "ent_punishment/*", // ES索引，统计字段
      path_prefix + "ent_recruit/*", // ES索引，统计字段
//      path_prefix + "ent_software/*", // ES索引，统计字段
//      path_prefix + "ent_top500/*", // 统计字段
      path_prefix + "ent_trademark/*", // ES索引，统计字段
      path_prefix + "ent_website/*", // ES索引，统计字段

      path_prefix + "ent_maimai/*", //
      path_prefix + "ent_zhaodao/*" //
    )
    val inputRDD: RDD[String] = sc.textFile(fileList.mkString(","))
//    inputRDD.collect().foreach(println(_))//ent_zhaodao|{"_id":{"$oid":"5fceccb9de514381b862b42f"},...}
    println("过滤前："+inputRDD.count()+"个")

    /**
      * 第一步：拼接
      * 根据entId拼接元组
      */
    val tupleRDD: RDD[(String, JSONObject)] = inputRDD.map(line => {
      try {
        val tableName = line.substring(0, line.indexOf("|")).trim()
        val content = line.substring(line.indexOf("{"), line.length())
        val jsonObj: JSONObject = JSONUtils.getNotNullJson(content)// 解析json字符串，并对日期字段单独处理
        jsonObj.put("tableName", tableName);
        val entId = jsonObj.getString("entId")
        if (entId.equals("empty")) { // 很多entId为empty字符串的值，需过滤，否则该key会造成严重的数据倾斜
          (null, null)
        } else {
          (entId, jsonObj)
        }
      } catch {
        case ex: Exception => (null, null)
      }
    })
      .filter(t => !StringUtils.isEmpty(t._1))
      .filter(t => !(t._2.getString("tableName").equals("ent_invest_company") && StringUtils.isEmpty(t._2.getString("isBrunch"))))
    println("第一次过滤后："+tupleRDD.count()+"个")

//    Thread.sleep(60*1000)

    /**
     * 第二步：聚合
     * 对entId进行聚合，然后遍历迭代器，将同一企业在不同表的JSON数据（需去重）放入JSONArray中
     * 再根据表名为key，JSONArray为值，构建map集合
     * 最后map转成一个大的JSONObject
     * 格式：{"ent": [{"entId": "5eaa3d2d581f7afded866558",...}，...}]，"ent_recruit": [{"entId": "5eaa3d2d581f7afded866558",...}],...}
     */
    import scala.collection.JavaConversions._
    val jsonRDD: RDD[JSONObject] = tupleRDD.groupByKey().map{case (entId,iter) => {
      var set = new util.HashSet[String]()
      var map = new util.HashMap[String, JSONArray]()

      for (jsonObj <- iter) {
        val tableName = jsonObj.getString("tableName")
        if (map.get(tableName) == null) {
          val array = new JSONArray()
          array.add(jsonObj)
          set.add(jsonObj.toJSONString)
          map.put(tableName, array)
        } else {
          val array = map.get(tableName)
          if (!set.contains(jsonObj.toJSONString)) { //去重
            array.add(jsonObj)
            set.add(jsonObj.toJSONString)
          }
          map.put(tableName, array)
        }
      }

      var finalJsonObj = new JSONObject();
      for (key <- map.keySet()) {
        finalJsonObj.put(key, map.get(key))
      }
      finalJsonObj
    }}
//      .filter(_.getJSONArray("ent") != null) // 如果一个entId对应的其它表有数据，但该企业在Ent表没有数据，则过滤 （todo 测试数据不足，暂时注释掉）

//    jsonRDD.collect().foreach(println(_))
    println("聚合处理后："+jsonRDD.count()+"个")

    // 广播dirver端变量
    val bc_categoryList: Broadcast[util.ArrayList[CategoryItem]] = sc.broadcast(GetCategory.getFinalCategoryList)

    /**
     * 第三步：数据清洗
     */
    val mongoRDD: RDD[JSONObject] = jsonRDD
      .map(MangoHandle.merge)
      .map(MangoHandle.customCategory(_, bc_categoryList.value))
      .map(MangoHandle.products)
      .map(MangoHandle.corpStatusString)
      .map(MangoHandle.agentType)
      .map(MangoHandle.check)
      .map(MangoHandle.setMangoId)
      .persist(StorageLevel.MEMORY_AND_DISK)

    //再次针对每一个表单独清洗，字段格式转换等等
    val esRDD: RDD[JSONObject] = jsonRDD.map(EsHandle.transforToEs)

    esRDD.collect().foreach(println(_))  //很多空对象 {}

    esRDD.count()
    // 往es写数据
//    esRDD.map(json => {
//      for (key <- json.keySet()) {
//        if ("ent".equals(key) ||
//          "ent_a_taxpayer".equals(key) ||
//          "ent_abnormal_opt".equals(key) ||
//          "ent_apps".equals(key) ||
//          "ent_bids".equals(key) ||
//          "ent_brand".equals(key) ||
//          "ent_cert".equals(key) ||
//          "ent_contacts".equals(key) ||
//          "ent_copyrights".equals(key) ||
//          "ent_court_notice".equals(key) ||
//          "ent_ecommerce".equals(key) ||
//          "ent_funding_event".equals(key) ||
//          "ent_goods".equals(key) ||
//          "ent_licence".equals(key) ||
//          "ent_new_media".equals(key) ||
//          "ent_news".equals(key) ||
//          "ent_patent".equals(key) ||
//          "ent_punishment".equals(key) ||
//          "ent_recruit".equals(key) ||
//          "ent_software".equals(key) ||
//          "ent_trademark".equals(key) ||
//          "ent_website".equals(key)
//        ) {
//          ElasticSearchUtil.postBatchByArray(key, json.getJSONArray(key))
//          //					ElasticSearchUtil.postBatchByArray("prod_" + key, json.getJSONArray(key))
//        }
//      }
//    }).count()















    // TODO: 也可以用直接父级目录
//    val path = "hdfs://hadoop102:9820/transform/ent/2020-12-04"
    import spark.implicits._
    // TODO: path一定要到分叉的那一层目录，否则就不不行！
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
//
//
//    val path_prefix = "D:\\JavaRelation\\工作\\安徽创瑞\\mongoDatas\\transform\\"
////    path不支持传string，用逗号分隔，而textFile支持
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
