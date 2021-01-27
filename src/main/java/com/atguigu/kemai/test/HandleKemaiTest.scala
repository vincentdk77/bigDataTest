package com.atguigu.kemai.test

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONArray, JSONException, JSONObject}
import com.atguigu.kemai.utils.ConnectionConstant
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable

object HandleKemaiTest {
  System.setProperty("HADOOP_USER_NAME","root")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("HandleKemaiTest")
//      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val path = ConnectionConstant.HDFS_URL + "/transform/" + "2020/11/03/*"

//    val inputDF: DataFrame = spark.read.text(path)
//    println(inputDF.rdd.getNumPartitions)  //9个分区

    val inputRDD: RDD[String] = sc.textFile(path)
//    println(inputRDD.getNumPartitions)  //60个分区

    //转换成元组 （entId为key）
    val entIdTupleRDD: RDD[(String, JSONObject)] = inputRDD.map(str => {

//      val array = str.split("\\|")// todo 不能用这种方式来处理，因为后面可能也会有"|"字符
//      val tableName = array(0)
//      val jsonStr = array(1)
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
      (entId, jsonObj)
    })

    //根据entId分组
    //格式：{"ent": [{"entId": "5eaa3d2d581f7afded866558",...}，...}]，"ent_recruit": [{"entId": "5eaa3d2d581f7afded866558",...}],...}
    val resultRDD: RDD[JSONObject] = entIdTupleRDD.groupByKey().map { case (entId, iter) => {
      val resultJsonObj: JSONObject = new JSONObject()

      val tableNametuples: Array[(String, JSONObject)] = iter.toArray.map(jsonObj => {
        val tableName: String = jsonObj.getString("tableName")
        // TODO: 将iter转成大json，每一个元素为tableName为key的jsonArray
        //        resultJsonObj.put(tableName,array)
        (tableName, jsonObj)
      })

      tableNametuples
        .groupBy(_._1)
        .map { case (tableName, array: Array[(String, JSONObject)]) => {
          val resultArray: Array[JSONObject] = array.map(_._2)
          try{
            // TODO: 将json对象的数组，转成JSONArray
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

      resultJsonObj
    }}

    resultRDD.take(20).foreach(println)

    val destPath: String = ConnectionConstant.HDFS_URL + "/destPath"
    resultRDD.saveAsTextFile(destPath)

    //todo  保存到ES


//    inputDF.show()


    spark.stop()
  }

}
