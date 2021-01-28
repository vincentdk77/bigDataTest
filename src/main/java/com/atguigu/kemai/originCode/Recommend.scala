package com.atguigu.kemai.originCode

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.atguigu.kemai.mango.{MangoHandle, MongoUtils}
import com.atguigu.kemai.recommend.{GetCategory, RecommendUtil}
import com.atguigu.kemai.utils.JSONUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.types.ObjectId

import java.util

object Recommend {
  def main(args: Array[String]): Unit = {

    // 参数校验
    if (args.length != 1) {
      println(
        """
				  |----------please enter params:----------
				  |- score: 达标分数
                """.stripMargin)
      sys.exit()
    }
    val Array(score) = args

    // 运行环境
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
    val sc = new SparkContext(conf)

    // 广播dirver端的变量
    val bc_score = sc.broadcast(score.toInt)
    val bc_categoryList = sc.broadcast(GetCategory.getFinalCategoryList)

    /** 只读和推荐维度有关的表
     */
    val fileList = Array(
      "hdfs://jtb/transform/2020/11/03/ent/*",
      "hdfs://jtb/transform/2020/11/03/ent_a_taxpayer/*",
      "hdfs://jtb/transform/2020/11/03/ent_abnormal_opt/*",
      "hdfs://jtb/transform/2020/11/03/ent_cert/*",
      "hdfs://jtb/transform/2020/11/03/ent_domain/*",
      "hdfs://jtb/transform/2020/11/03/ent_growingup/*",
      "hdfs://jtb/transform/2020/11/03/ent_listed/*",
      "hdfs://jtb/transform/2020/11/03/ent_recruit/*",
      "hdfs://jtb/transform/2020/11/03/ent_top500/*"
    )

    //		val inputRDD = sc.textFile("hdfs://jtb/transform/2020/11/03/ent/*")
    val inputRDD = sc.textFile(fileList.mkString(","))

    val tupleRDD: RDD[(String, JSONObject)] = inputRDD.map(line => {
      try {
        val tableName = line.substring(0, line.indexOf("|")).trim()
        val content = line.substring(line.indexOf("{"), line.length())
        val json = JSONUtils.getNotNullJson(content)
        json.put("tableName", tableName);
        val entId = json.getString("entId")
        if (entId.equals("empty")) { // 很多entId为empty字符串的值，需过滤，否则该key会造成严重的数据倾斜
          (null, null)
        } else {
          (entId, json)
        }
      } catch {
        case ex: Exception => (null, null)
      }
    }).filter(t => !StringUtils.isEmpty(t._1))

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
          if (!set.contains(json.toJSONString)) {
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
    })
      .filter(_.getJSONArray("ent") != null)
      .map(MangoHandle.merge)
      .map(MangoHandle.customCategory(_, bc_categoryList.value))
      .map(MangoHandle.products)
      .map(MangoHandle.corpStatusString)
      .map(MangoHandle.agentType)
      .map(MangoHandle.check)
      .map(MangoHandle.setMangoId)
      .map(_.getJSONArray("ent").getJSONObject(0))
      .filter(json => StringUtils.isNotEmpty(json.getString("opScope")))
      .filter(json => {
        val corpStatusString = json.getString("corpStatusString")
        if (StringUtils.isNotEmpty(corpStatusString)) {
          if (corpStatusString.startsWith("注销") || corpStatusString.startsWith("吊销")) {
            false
          } else {
            true
          }
        } else {
          true
        }
      })
      //			.filter(json => {
      //				val regProv = json.getString("regProv")
      //				if (StringUtils.isNotEmpty(regProv)) {
      //					if (regProv.startsWith("安徽")) {
      //						true
      //					} else {
      //						false
      //					}
      //				} else {
      //					false
      //				}
      //			})
      .coalesce(240, shuffle = true) //缩减分区数，让每个分区数据量相对均匀  todo	一般合并分区数不需要进行shuffle

    println("--------------------------------------------------jsonRDD--------------------------------------------------")

    val mongoRDD: RDD[(String, JSONObject)] = jsonRDD.map(json => {

      val industryCategory = json.getString("industryCategory")

      if (StringUtils.isNotEmpty(industryCategory)) {
        val words: Array[String] = industryCategory.split(",")

        /**
         * word(0)作为key会存在严重的数据倾斜，这里采用随机后缀的方式来打散key
         * 总计：31512426
         * (电力、热力、燃气及水生产和供应业,82917)
         * (批发和零售业,12731921)
         * (房地产业,719503)
         * (文化、体育和娱乐业,330375)
         * (水利、环境和公共设施管理业,112619)
         * (卫生和社会工作,45808)
         * (国际组织,208)
         * (租赁和商务服务业,3553630)
         * (住宿和餐饮业,1361901)
         * (制造业,4151372)
         * (信息传输、软件和信息技术服务业,1240645)
         * (科学研究和技术服务业,1832334)
         * (建筑业,1920922)
         * (交通运输、仓储和邮政业,797464)
         * (农、林、牧、渔业,817200)
         * (金融业,281544)
         * (教育,140219)
         * (采矿业,53471)
         * (居民服务、修理和其他服务业,1337286)
         * (公共管理、社会保障和社会组织,1087)
         */
        if (words(0).equals("电力、热力、燃气及水生产和供应业")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(9) //82917
          (newKey, json)
        }
        else if (words(0).equals("批发和零售业")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(1274) //12731921
          (newKey, json)
        }
        else if (words(0).equals("房地产业")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(72) //719503
          (newKey, json)
        }
        else if (words(0).equals("文化、体育和娱乐业")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(33) //330375
          (newKey, json)
        }
        else if (words(0).equals("水利、环境和公共设施管理业")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(12) //112619
          (newKey, json)
        }
        else if (words(0).equals("卫生和社会工作")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(5) //45808
          (newKey, json)
        }
        else if (words(0).equals("租赁和商务服务业")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(356) //3553630
          (newKey, json)
        }
        else if (words(0).equals("住宿和餐饮业")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(136) //1361901
          (newKey, json)
        }
        else if (words(0).equals("制造业")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(415) //4151372
          (newKey, json)
        }
        else if (words(0).equals("信息传输、软件和信息技术服务业")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(124) //1240645
          (newKey, json)
        }
        else if (words(0).equals("科学研究和技术服务业")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(183) //1832334
          (newKey, json)
        }
        else if (words(0).equals("建筑业")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(192) //1920922
          (newKey, json)
        }
        else if (words(0).equals("交通运输、仓储和邮政业")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(80) //797464
          (newKey, json)
        }
        else if (words(0).equals("农、林、牧、渔业")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(82) //817200
          (newKey, json)
        }
        else if (words(0).equals("金融业")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(28) //281544
          (newKey, json)
        }
        else if (words(0).equals("教育")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(14) //140219
          (newKey, json)
        }
        else if (words(0).equals("采矿业")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(6) //53471
          (newKey, json)
        }
        else if (words(0).equals("居民服务、修理和其他服务业")) {
          val newKey = words(0) + "_" + scala.util.Random.nextInt(134) //1337286
          (newKey, json)
        }
        else {
          (words(0), json)
        }
      } else { // 所有数据必须要有industryCategory字段，否则都是垃圾数据
        (null, null)
      }
    })
      .filter(t => StringUtils.isNotEmpty(t._1))

    //		val size = mongoRDD.countByKey().size
    //		println("--------------------------------------------------总计共有" + size + "个分类--------------------------------------------------")
    //		val map = mongoRDD.countByKey()
    //		println("--------------------------------------------------分类--------------------------------------------------")
    //		map.foreach(println)

    //根据“行业分类”分组
    val similarRDD: RDD[(String, util.HashSet[String])] = mongoRDD.groupByKey().flatMap { case (categoryName, iter) => {
      val list = new util.ArrayList[JSONObject]()
      for (jsonObj <- iter) {
        list.add(jsonObj)
      }
      // 返回相似企业集合
      val map: util.Map[String, util.HashSet[String]] = RecommendUtil.getRecommend(list, bc_score.value)
      val array = new Array[(String, util.HashSet[String])](map.size());
      var i = 0;
      for (entId <- map.keySet()) {
        array(i) = (entId, map.get(entId))
        i = i + 1
      }
      array
    }
    }

    similarRDD.mapPartitions(similarFunc).count()
    sc.stop()
  }

  /**
   * 相似企业数据，存入Mongo
   *
   * @param iter
   * @return
   */
  def similarFunc(iter: Iterator[(String, util.HashSet[String])]): Iterator[(String, util.HashSet[String])] = {
    var index: Int = 0
    var map: util.Map[String, util.Collection[JSONObject]] = new util.HashMap[String, util.Collection[JSONObject]]
    var list: util.List[JSONObject] = new util.ArrayList[JSONObject]

    while (iter.hasNext) {
      val e: (String, util.HashSet[String]) = iter.next
      val entId = e._1
      val similarIdAndScore: String = e._2.toString
      var json = new JSONObject()
      json.put("_id", new ObjectId(entId)) // 将entId作为主键
      json.put("entId", entId)
      json.put("similarId", similarIdAndScore)
      list.add(json)
      index = index + 1

      //每500条存一次
      if (index % 500 == 0) {
        map.put("ent_similar_70", list)
        MongoUtils.batchInsert(map)
        map.clear()
        list.clear()
      }
    }
    if (list.size() > 0) {
      map.put("ent_similar_70", list)
      MongoUtils.batchInsert(map)
      map.clear()
      list.clear()
    }
    iter
  }
}
