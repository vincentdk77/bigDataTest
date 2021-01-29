package com.atguigu.kemai.test

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONArray, JSONException, JSONObject}
import com.atguigu.kemai.utils.ConnectionConstant
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util
import scala.collection.mutable.ArrayBuffer

object HandleKemaiTest_mapPartitions {
  System.setProperty("HADOOP_USER_NAME","root")

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val spark = SparkSession.builder().appName("HandleKemaiTest")
//      .master("local[*]")
      .config("spark.default.parallelism","500")//只在shuffle的时候生效，shuffle算子开始的分区数为这个值
      .getOrCreate()

    val sc = spark.sparkContext

//    val path = ConnectionConstant.HDFS_URL + "/transform/" + "2021/01/07/*"
    val path = ConnectionConstant.HDFS_URL + "/transform/" + "2020/11/03/*"

//    val inputDF: DataFrame = spark.read.text(path)
//    println(inputDF.rdd.getNumPartitions)  //9个分区

    val inputRDD: RDD[String] = sc.textFile(path)
//    println("textFile后的分区数："+inputRDD.getNumPartitions)  //60个分区

    //转换成元组 （entId为key）
    val entIdTupleRDD: RDD[(String, JSONObject)] = inputRDD.repartition(500).mapPartitions(iter => {

//      val array = str.split("\\|")//不能用这种方式来处理，因为后面可能也会有"|"字符
//      val tableName = array(0)
//      val jsonStr = array(1)
      val list =ArrayBuffer[(String, JSONObject)]()
      for (str <- iter) {

        val tableName = str.substring(0,str.indexOf("|"))
        val jsonStr = str.substring(str.indexOf("|")+1)

        var jsonObj: JSONObject = null
        var entId:String = null
        try {
          jsonObj = JSON.parseObject(jsonStr)
          jsonObj.put("tableName",tableName)
          entId = jsonObj.getString("entId")
        }catch{
          case e:JSONException => {
            println("异常str："+str)
            println("异常JSON："+jsonStr)
            e.printStackTrace()
          }
        }
        list.append((entId, jsonObj))
      }
      list.iterator
    })

    //根据entId分组
    //格式：{"ent": [{"entId": "5eaa3d2d581f7afded866558",...}，...}]，"ent_recruit": [{"entId": "5eaa3d2d581f7afded866558",...}],...}
    val groupbyRDD: RDD[(String, Iterable[JSONObject])] = entIdTupleRDD.groupByKey()
//    println("group by 分区数:"+groupbyRDD.getNumPartitions)  不改变分区数！

    val resultRDD: RDD[JSONObject] = groupbyRDD.mapPartitions{ itera => {
      val list =ArrayBuffer[JSONObject]()
      for ((entId, iter) <- itera) {
        val resultJsonObj: JSONObject = new JSONObject()

        val tableNametuples: Array[(String, JSONObject)] = iter.toArray.map(jsonObj => {
          val tableName: String = jsonObj.getString("tableName")
          (tableName, jsonObj)
        })

        tableNametuples
          .groupBy(_._1)
          .map { case (tableName, array: Array[(String, JSONObject)]) => {
            val resultArray: Array[JSONObject] = array.map(_._2)
            try{
              //将json对象的数组，转成JSONArray
              val jsonArray: JSONArray = JSON.parseArray(JSON.toJSONString(resultArray,SerializerFeature.QuoteFieldNames))
              resultJsonObj.put(tableName, jsonArray)
            }catch{
              case e:Exception =>{
                println("JSON.parseArray错误！json="+resultArray.mkString(","))
                e.printStackTrace()
              }
            }
            resultJsonObj
          }}

        list.append(resultJsonObj)
      }
      list.iterator
    }}
    .filter(_.getJSONArray("ent") != null)
//    .filter(_.size()>1)
//    .cache()

//    println("共"+resultRDD.count()+"条数据!")
//    resultRDD.take(20).foreach(println)

    val destPath: String = ConnectionConstant.HDFS_URL + "/destPath/2021-01-07"
    resultRDD.saveAsTextFile(destPath)

    //todo  保存到ES


//    inputDF.show()


    spark.stop()
  }

}
