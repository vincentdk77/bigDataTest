package com.atguigu.kemai

import java.util

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.atguigu.kemai.es.{ElasticSearchUtil, EsHandle}
import com.atguigu.kemai.mango.MangoHandle
import com.atguigu.kemai.recommend.GetCategory
import com.atguigu.kemai.utils.JSONUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object HandleKemai {
	def main(args: Array[String]): Unit = {

		// 参数校验
		//		if (args.length != 1) {
		//			println(
		//				"""
		//				  |----------please enter params:----------
		//				  |- executeCode: 执行代码
		//				  |  - 10: mongo,
		//				  |  - 01: es,
		//				  |  - 11: mongo+es
		//		                """.stripMargin)
		//			sys.exit()
		//	}
		//		var Array(isRerun, date, executeCode) = args

		//		if (isRerun.equals("no")) {
		//			val format = new SimpleDateFormat("yyyy-MM-dd")
		//			date = format.format(new Date()).replace("-", "/")
		//		}

		// 运行环境
		val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
		val sc = new SparkContext(conf)

		// 广播dirver端变量
		val bc_categoryList = sc.broadcast(GetCategory.getFinalCategoryList)

		// 读取数据
		//		val fileList = Array("hdfs://jtb/transform/2020/11/03/*", "hdfs://jtb/transform/2020/11/16/*")

		val path_prefix = "hdfs://jtb/transform/"
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
			path_prefix + "ent_growingup/*", // 统计字段
			path_prefix + "ent_invest_company/*", // 统计字段
			path_prefix + "ent_licence/*", // ES索引，统计字段
			path_prefix + "ent_listed/*", // 统计字段
			path_prefix + "ent_new_media/*", // ES索引，统计字段
			path_prefix + "ent_news/*", // ES索引，统计字段
			path_prefix + "ent_patent/*", // ES索引，统计字段
			path_prefix + "ent_punishment/*", // ES索引，统计字段
			path_prefix + "ent_recruit/*", // ES索引，统计字段
			path_prefix + "ent_software/*", // ES索引，统计字段
			path_prefix + "ent_top500/*", // 统计字段
			path_prefix + "ent_trademark/*", // ES索引，统计字段
			path_prefix + "ent_website/*", // ES索引，统计字段

			path_prefix + "ent_maimai/*", //
			path_prefix + "ent_zhaodao/*" //
		)

		val inputRDD: RDD[String] = sc.textFile(fileList.mkString(","))

		/**
		 * 第一步：拼接
		 * 根据entId拼接元组
		 */
		val tupleRDD: RDD[(String, JSONObject)] = inputRDD.map(line => {
			try {
				val tableName = line.substring(0, line.indexOf("|")).trim()
				val content = line.substring(line.indexOf("{"), line.length())
				val json = JSONUtils.getNotNullJson(content) // 简单的数据格式处理
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
		})
			.filter(t => !StringUtils.isEmpty(t._1))
			.filter(t => !(t._2.getString("tableName").equals("ent_invest_company") && StringUtils.isEmpty(t._2.getString("isBrunch"))))

		import scala.collection.JavaConversions._

		/**
		 * 第二步：聚合
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

		val esRDD: RDD[JSONObject] = jsonRDD.map(EsHandle.transforToEs)

		// 往es写数据
		esRDD.map(json => {
			for (key <- json.keySet()) {
				if ("ent".equals(key) ||
					"ent_a_taxpayer".equals(key) ||
					"ent_abnormal_opt".equals(key) ||
					"ent_apps".equals(key) ||
					"ent_bids".equals(key) ||
					"ent_brand".equals(key) ||
					"ent_cert".equals(key) ||
					"ent_contacts".equals(key) ||
					"ent_copyrights".equals(key) ||
					"ent_court_notice".equals(key) ||
					"ent_ecommerce".equals(key) ||
					"ent_funding_event".equals(key) ||
					"ent_goods".equals(key) ||
					"ent_licence".equals(key) ||
					"ent_new_media".equals(key) ||
					"ent_news".equals(key) ||
					"ent_patent".equals(key) ||
					"ent_punishment".equals(key) ||
					"ent_recruit".equals(key) ||
					"ent_software".equals(key) ||
					"ent_trademark".equals(key) ||
					"ent_website".equals(key)
				) {
					ElasticSearchUtil.postBatchByArray(key, json.getJSONArray(key))
					//					ElasticSearchUtil.postBatchByArray("prod_" + key, json.getJSONArray(key))
				}
			}
		}).count()

		// 父子文档
		//        if (executeCode.charAt(1) == '1') {
		//            val esRDD: RDD[JSONObject] = jsonRDD.map(EsHandle.transforToEs)
		//            //            esRDD.collect().foreach(println)
		//
		//            esRDD.map(json => {
		//                //先创建空index，并设置mapping
		//                //                ElasticSearchUtil.createMapping("test")
		//                /**
		//                 * 先用HTTP请求，设置mapping
		//                 * PUT test
		//                 * {
		//                 * "mappings": {
		//                 * "properties": {
		//                 * "parent_child": {
		//                 * "type": "join",
		//                 * "relations": {
		//                 * "parent": "child"
		//                 * }
		//                 * }
		//                 * }
		//                 * }
		//                 * }
		//                 */
		//                //插入父文档
		//                ElasticSearchUtil.postParentDocument("test", json.getJSONObject("ent"))
		//                import scala.collection.JavaConversions._
		//                val tablenames = json.keySet()
		//                //插入子文档
		//                for (key <- tablenames) {
		//                    if ("ent_contacts".equals(key) ||
		//                        "ent_website".equals(key) ||
		//                        "ent_promotion".equals(key) ||
		//                        "ent_ecommerce".equals(key) ||
		//                        "ent_abnormal_opt".equals(key) ||
		//                        "ent_brand".equals(key) ||
		//                        "ent_patent".equals(key) ||
		//                        "ent_funding_event".equals(key)) {
		//                        ElasticSearchUtil.postChildDocumentByJson("test", json.getJSONObject(key))
		//
		//                    } else if ("ent_recruit".equals(key) ||
		//                        "ent_cert".equals(key) ||
		//                        "ent_news".equals(key) ||
		//                        "ent_software".equals(key) ||
		//                        "ent_licence".equals(key) ||
		//                        "ent_ecommerce".equals(key) ||
		//                        "ent_new_media".equals(key) ||
		//                        "ent_goods".equals(key) ||
		//                        "ent_lawsuit".equals(key) ||
		//                        "ent_trademark".equals(key)) {
		//                        ElasticSearchUtil.postChildDocumentByArray("test", json.getJSONArray(key))
		//                    }
		//                }
		//            }).count()
		//        }

		sc.stop()
	}
}