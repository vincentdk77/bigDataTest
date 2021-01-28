package com.atguigu.kemai.originCode

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.atguigu.kemai.mango.{MangoCount, MangoHandle, MongoUtils}
import com.atguigu.kemai.recommend.GetCategory
import com.atguigu.kemai.utils.JSONUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import java.util

object HandleKemaiCount {
    def main(args: Array[String]): Unit = {

        // 运行环境
        val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
        val sc = new SparkContext(conf)

        // 广播dirver端变量
        val bc_categoryList = sc.broadcast(GetCategory.getFinalCategoryList)

        // 读取数据
        val fileList = Array("hdfs://jtb/transform/2020/11/03/*", "hdfs://jtb/transform/2020/11/16/*")
        val inputRDD: RDD[String] = sc.textFile(fileList.mkString(","))

        // 根据entId拼接元组
        val tupleRDD: RDD[(String, JSONObject)] = inputRDD.map(line => {
            try {
                val tableName = line.substring(0, line.indexOf("|")).trim()
                val content = line.substring(line.indexOf("{"), line.length())
                val json = JSONUtils.getNotNullJson(content) // 简单的数据格式处理
                json.put("tableName", tableName);
                val entId = json.getString("entId")
                if (entId.equals("empty")) {	// 很多entId为empty字符串的值，需过滤，否则该key会造成严重的数据倾斜
                    (null, null)
                } else {
                    (entId, json)
                }
            } catch {
                case ex: Exception => (null, null)
            }
        }).filter(t => !StringUtils.isEmpty(t._1))
            .filter(t => !(t._2.getString("tableName").equals("ent_invest_company") && StringUtils.isEmpty(t._2.getString("isBrunch"))))

        import scala.collection.JavaConversions._

        /**
         * 对entId进行聚合，然后遍历迭代器，将同一企业在不同表的JSON数据（需去重）放入JSONArray中
         * 再根据表名为key，JSONArray为值，构建map集合
         * 最后map转成一个大的JSONObject
         * 格式：{"ent": [{"entId": "5eaa3d2d581f7afded866558",...}，...}]，"ent_recruit": [{"entId": "5eaa3d2d581f7afded866558",...}],...}
         */
        val jsonRDD: RDD[JSONObject] = tupleRDD.groupByKey().map(t => {
            var set = new util.HashSet[String]()
            var map = new util.HashMap[String, JSONArray]()

            for (json <- t._2) {
                val tableName = json.getString("tableName")
                if (map.get(tableName) == null) {
                    val array = new JSONArray()
                    array.add(json)
                    set.add(json.toJSONString)
                    map.put(tableName, array)
                } else {
                    val array = map.get(tableName)
                    if (!set.contains(json.toJSONString)) { //去重
                        array.add(json)
                        set.add(json.toJSONString)
                    }
                    map.put(tableName, array)
                }
            }

            var finalJson = new JSONObject();
            for (key <- map.keySet()) {
                finalJson.put(key, map.get(key))
            }
            finalJson
        }).filter(_.getJSONArray("ent") != null) // 如果其它表有数据，但该企业在Ent表没有数据，则过滤

        val mongoRDD: RDD[JSONObject] = jsonRDD
            .map(MangoHandle.merge)
            .map(MangoHandle.customCategory(_, bc_categoryList.value))
            .map(MangoHandle.products)
            .map(MangoHandle.corpStatusString)
            .map(MangoHandle.agentType)
            .map(MangoHandle.check)
            .map(MangoHandle.setMangoId)
            .map(MangoCount.jsonCount)
            .persist(StorageLevel.MEMORY_AND_DISK)

        mongoRDD.mapPartitions(func).count()     //分区插入，减少数据库连接资源消耗
        sc.stop()
    }

    def func(iter: Iterator[JSONObject]): Iterator[JSONObject] = {
        var index = 0
        var list: util.ArrayList[JSONObject] = new util.ArrayList[JSONObject]
        for (json <- iter) {
            list.add(json)
            index = index + 1
            if (index % 2000 == 0) {    //每2000个插入1次
                MongoUtils.batchInsertListNew(list)
                list.clear()
            }
        }
        //若最后一批不足2000，将添加到list的json一起插入mongo
        if (list.size() > 0) {
            MongoUtils.batchInsertListNew(list)
        }
        iter
    }
}