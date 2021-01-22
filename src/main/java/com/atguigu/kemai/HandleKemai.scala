//package com.atguigu.kemai
//
//import java.util
//
//import com.alibaba.fastjson.{JSONArray, JSONObject}
//import org.apache.commons.lang3.StringUtils
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//object HandleKemai {
//	def main(args: Array[String]): Unit = {
//
//		// 参数校验
//		//		if (args.length != 1) {
//		//			println(
//		//				"""
//		//				  |----------please enter params:----------
//		//				  |- executeCode: 执行代码
//		//				  |  - 10: mongo,
//		//				  |  - 01: es,
//		//				  |  - 11: mongo+es
//		//		                """.stripMargin)
//		//			sys.exit()
//		//	}
//		//		var Array(isRerun, date, executeCode) = args
//
//		//		if (isRerun.equals("no")) {
//		//			val format = new SimpleDateFormat("yyyy-MM-dd")
//		//			date = format.format(new Date()).replace("-", "/")
//		//		}
//
//		// 运行环境
//		val conf = new SparkConf().setMaster("local[*]").setAppName(s"${this.getClass.getSimpleName}")
//		val sc = new SparkContext(conf)
//
//		// 广播dirver端变量
//		val bc_categoryList = sc.broadcast(GetCategory.getFinalCategoryList)
//
//		// 读取数据
//		//		val fileList = Array("hdfs://jtb/transform/2020/11/03/*", "hdfs://jtb/transform/2020/11/16/*")
//
////		val path_prefix = "hdfs://jtb/transform/"
////		val fileList = Array(
////			path_prefix + "ent/*", // ES索引，统计字段
////			path_prefix + "ent_a_taxpayer/*", // ES索引，统计字段
////			path_prefix + "ent_abnormal_opt/*", // ES索引
////			path_prefix + "ent_annual_report/*", // 统计字段
////			path_prefix + "ent_apps/*", // ES索引，统计字段
////			path_prefix + "ent_bids/*", // ES索引
////			path_prefix + "ent_brand/*", // ES索引
////			path_prefix + "ent_cert/*", // ES索引，统计字段
////			path_prefix + "ent_contacts/*", // ES索引，统计字段
////			path_prefix + "ent_copyrights/*", // ES索引
////			path_prefix + "ent_court_notice/*", // 统计字段
////			path_prefix + "ent_court_operator/*", // ES索引
////			path_prefix + "ent_court_paper/*", // 统计字段
////			path_prefix + "ent_dishonesty_operator/*", //
////			path_prefix + "ent_ecommerce/*", // ES索引，统计字段
////			path_prefix + "ent_equity_pledged/*", // 统计字段
////			path_prefix + "ent_funding_event/*", // ES索引
////			path_prefix + "ent_goods/*", // ES索引
////			path_prefix + "ent_growingup/*", // 统计字段
////			path_prefix + "ent_invest_company/*", // 统计字段
////			path_prefix + "ent_licence/*", // ES索引，统计字段
////			path_prefix + "ent_listed/*", // 统计字段
////			path_prefix + "ent_new_media/*", // ES索引，统计字段
////			path_prefix + "ent_news/*", // ES索引，统计字段
////			path_prefix + "ent_patent/*", // ES索引，统计字段
////			path_prefix + "ent_punishment/*", // ES索引，统计字段
////			path_prefix + "ent_recruit/*", // ES索引，统计字段
////			path_prefix + "ent_software/*", // ES索引，统计字段
////			path_prefix + "ent_top500/*", // 统计字段
////			path_prefix + "ent_trademark/*", // ES索引，统计字段
////			path_prefix + "ent_website/*", // ES索引，统计字段
////
////			path_prefix + "ent_maimai/*", //
////			path_prefix + "ent_zhaodao/*" //
////		)
////		val inputRDD: RDD[String] = sc.textFile(fileList.mkString(","))
//		val path_prefix = "D:\\JavaRelation\\工作\\安徽创瑞\\mongoDatas\\transform\\"
//		val fileList = Array(path_prefix + "ent_top500\\*")
//		val path = fileList.mkString(",")
//		val inputRDD: RDD[String] = sc.textFile(path)
////		val inputRDD: RDD[String] = sc.textFile("D:\\JavaRelation\\工作\\安徽创瑞\\mongoDatas\\transform\\ent\\2020-11-21\\*")
////		inputRDD.collect().foreach(println(_))
//
////		ent|{"_id":{"$oid":"5f856f0f8034b3c2d705f647"},"uniscId":"91211103MA10E6140W","entTypeCN":"有限责任公司（自然人投资或控股）","entName":"盘锦信朋缘劳务有限公司","corpStatusString":"存续（在营、开业、在册）","estDate":"2020-06-04","apprDate":"2020-06-04","regOrgCn":"盘锦市兴隆台区市场监督管理局","regCapCurCN":"人民币","regCaption":100.0,"regCap":"壹佰万元整","legalName":"张艳阳","opFrom":"2020-06-04","opTo":"2040-06-03","regProv":"辽宁省","regCity":"盘锦市","regDistrict":"兴隆台区","opScope":"许可项目：建筑劳务分包，住宅室内装饰装修，各类工程建设活动，消防设施工程（依法须经批准的项目，经相关部门批准后方可开展经营活动，具体经营项目以审批结果为准） 一般项目：园林绿化工程施工，家政服务，五金产品零售，日用百货销售，家用电器销售，建筑装饰材料销售，机械设备租赁，消防器材销售（除依法须经批准的项目外，凭营业执照依法自主开展经营活动）","dom":"辽宁省盘锦市兴隆台区泰山路东、大众街北王家荒回迁楼3#11003","source":"爱企查公众号","spider":"aiqichaapp","q":91,"hidden":0,"createdAt":1602580239,"entId":"5f856f0f8034b3c2d705f647","desc":"盘锦信朋缘劳务有限公司成立于2020年6月4日，法定代表人为张艳阳，注册资本为100.0万元,统一社会信用代码为91211103MA10E6140W，企业地址位于辽宁省盘锦市兴隆台区泰山路东、大众街北王家荒回迁楼3#11003，经营范围包括：许可项目：建筑劳务分包，住宅室内装饰装修，各类工程建设活动，消防设施工程（依法须经批准的项目，经相关部门批准后方可开展经营活动，具体经营项目以审批结果为准） 一般项目：园林绿化工程施工，家政服务，五金产品零售，日用百货销售，家用电器销售，建筑装饰材料销售，机械设备租赁，消防器材销售（除依法须经批准的项目外，凭营业执照依法自主开展经营活动）。盘锦信朋缘劳务有限公司目前的经营状态为存续（在营、开业、在册）。"}
//
////		{
////			"_id": {
////				"$oid": "5f856f0f8034b3c2d705f647"
////			},
////			"uniscId": "91211103MA10E6140W",
////			"entTypeCN": "有限责任公司（自然人投资或控股）",
////			"entName": "盘锦信朋缘劳务有限公司",
////			"corpStatusString": "存续（在营、开业、在册）",
////			"estDate": "2020-06-04",
////			"apprDate": "2020-06-04",
////			"regOrgCn": "盘锦市兴隆台区市场监督管理局",
////			"regCapCurCN": "人民币",
////			"regCaption": 100.0,
////			"regCap": "壹佰万元整",
////			"legalName": "张艳阳",
////			"opFrom": "2020-06-04",
////			"opTo": "2040-06-03",
////			"regProv": "辽宁省",
////			"regCity": "盘锦市",
////			"regDistrict": "兴隆台区",
////			"opScope": "许可项目：建筑劳务分包，住宅室内装饰装修，各类工程建设活动，消防设施工程（依法须经批准的项目，经相关部门批准后方可开展经营活动，具体经营项目以审批结果为准） 一般项目：园林绿化工程施工，家政服务，五金产品零售，日用百货销售，家用电器销售，建筑装饰材料销售，机械设备租赁，消防器材销售（除依法须经批准的项目外，凭营业执照依法自主开展经营活动）",
////			"dom": "辽宁省盘锦市兴隆台区泰山路东、大众街北王家荒回迁楼3#11003",
////			"source": "爱企查公众号",
////			"spider": "aiqichaapp",
////			"q": 91,
////			"hidden": 0,
////			"createdAt": 1602580239,
////			"entId": "5f856f0f8034b3c2d705f647",
////			"desc": "盘锦信朋缘劳务有限公司成立于2020年6月4日，法定代表人为张艳阳，注册资本为100.0万元,统一社会信用代码为91211103MA10E6140W，企业地址位于辽宁省盘锦市兴隆台区泰山路东、大众街北王家荒回迁楼3#11003，经营范围包括：许可项目：建筑劳务分包，住宅室内装饰装修，各类工程建设活动，消防设施工程（依法须经批准的项目，经相关部门批准后方可开展经营活动，具体经营项目以审批结果为准） 一般项目：园林绿化工程施工，家政服务，五金产品零售，日用百货销售，家用电器销售，建筑装饰材料销售，机械设备租赁，消防器材销售（除依法须经批准的项目外，凭营业执照依法自主开展经营活动）。盘锦信朋缘劳务有限公司目前的经营状态为存续（在营、开业、在册）。"
////		}
//
//		/**
//		 * 第一步：拼接
//		 * 根据entId拼接元组
//		 */
//		val tupleRDD: RDD[(String, JSONObject)] = inputRDD.map(line => {
//			try {
//				val tableName = line.substring(0, line.indexOf("|")).trim()
//				val content = line.substring(line.indexOf("{"), line.length())
//				val json = JSONUtils.getNotNullJson(content) // 简单的数据格式处理
//				json.put("tableName", tableName);
//				val entId = json.getString("entId")
//				if (entId.equals("empty")) { // 很多entId为empty字符串的值，需过滤，否则该key会造成严重的数据倾斜
//					(null, null)
//				} else {
//					(entId, json)
//				}
//			} catch {
//				case ex: Exception => (null, null)
//			}
//		})
//			.filter(t => !StringUtils.isEmpty(t._1))
//			.filter(t => !(t._2.getString("tableName").equals("ent_invest_company") && StringUtils.isEmpty(t._2.getString("isBrunch"))))
//
//		tupleRDD.collect().foreach(println(_))
////		(5f2bbf61479818904158dbe3,{"regNo":"350122100124694","estDate":"2020-03-23","opScope":"互联网零售；农业园艺服务；花卉种植；陶瓷、石材装饰材料零售；纺织品及针织品零售；服装零售；鞋帽零售；化妆品及卫生用品零售；钟表、眼镜零售；箱包零售；厨具卫具零售；日用杂品零售；自行车等代步设备零售；文具用品零售；体育用品及器材零售（不含弩）；户外装备零售；珠宝首饰零售；工艺美术品及收藏品零售（象牙及其制品除外）；乐器零售；玩具专门零售；日用家电零售；其他电子产品零售；五金零售；灯具零售；通信设备（不含无线发射装置及卫星地面接收设施）零售。（依法须经批准的项目，经相关部门批准后方可开展经营活动）","dom":"福建省福州市连江县凤城镇马祖东路3号碧水龙城7号楼1305单元","entName":"连江县陌上贸易有限公司","hidden":"0","regCaption":"100.0","source":"企信通","tableName":"ent","uniscId":"91350122MA33N5WU3D","legalName":"郑青霞","sourceUrl":"https://www.qixintong.cn/company/6273355659696b474c4d65","createdAt":"2020-08-06 16:29:21","regCap":"壹佰万元整","taxNo":"91350122MA33N5WU3D","entId":"5f2bbf61479818904158dbe3","regCapCurCN":"人民币","regProv":"福建省","apprDate":"2020-03-23","corpStatusString":"存续（在营、开业、在册）","regDistrict":"连江县","q":"91","checkedAt":"2020-09-28 15:54:01","regCity":"福州市","entTypeCN":"有限责任公司（自然人投资或控股）","_id":"{\"$oid\":\"5f2bbf61479818904158dbe3\"}","spider":"qixintong","desc":"连江县陌上贸易有限公司成立于2020年3月23日，法定代表人为郑青霞，注册资本为100.0万元,统一社会信用代码为91350122MA33N5WU3D，企业地址位于福建省福州市连江县凤城镇马祖东路3号碧水龙城7号楼1305单元，经营范围包括：互联网零售；农业园艺服务；花卉种植；陶瓷、石材装饰材料零售；纺织品及针织品零售；服装零售；鞋帽零售；化妆品及卫生用品零售；钟表、眼镜零售；箱包零售；厨具卫具零售；日用杂品零售；自行车等代步设备零售；文具用品零售；体育用品及器材零售（不含弩）；户外装备零售；珠宝首饰零售；工艺美术品及收藏品零售（象牙及其制品除外）；乐器零售；玩具专门零售；日用家电零售；其他电子产品零售；五金零售；灯具零售；通信设备（不含无线发射装置及卫星地面接收设施）零售。（依法须经批准的项目，经相关部门批准后方可开展经营活动）。连江县陌上贸易有限公司目前的经营状态为存续（在营、开业、在册）。"})
//
////		{
////			"regNo": "350122100124694",
////			"estDate": "2020-03-23",
////			"opScope": "互联网零售；农业园艺服务；花卉种植；陶瓷、石材装饰材料零售；纺织品及针织品零售；服装零售；鞋帽零售；化妆品及卫生用品零售；钟表、眼镜零售；箱包零售；厨具卫具零售；日用杂品零售；自行车等代步设备零售；文具用品零售；体育用品及器材零售（不含弩）；户外装备零售；珠宝首饰零售；工艺美术品及收藏品零售（象牙及其制品除外）；乐器零售；玩具专门零售；日用家电零售；其他电子产品零售；五金零售；灯具零售；通信设备（不含无线发射装置及卫星地面接收设施）零售。（依法须经批准的项目，经相关部门批准后方可开展经营活动）",
////			"dom": "福建省福州市连江县凤城镇马祖东路3号碧水龙城7号楼1305单元",
////			"entName": "连江县陌上贸易有限公司",
////			"hidden": "0",
////			"regCaption": "100.0",
////			"source": "企信通",
////			"tableName": "ent",
////			"uniscId": "91350122MA33N5WU3D",
////			"legalName": "郑青霞",
////			"sourceUrl": "https://www.qixintong.cn/company/6273355659696b474c4d65",
////			"createdAt": "2020-08-06 16:29:21",
////			"regCap": "壹佰万元整",
////			"taxNo": "91350122MA33N5WU3D",
////			"entId": "5f2bbf61479818904158dbe3",
////			"regCapCurCN": "人民币",
////			"regProv": "福建省",
////			"apprDate": "2020-03-23",
////			"corpStatusString": "存续（在营、开业、在册）",
////			"regDistrict": "连江县",
////			"q": "91",
////			"checkedAt": "2020-09-28 15:54:01",
////			"regCity": "福州市",
////			"entTypeCN": "有限责任公司（自然人投资或控股）",
////			"_id": "{\"$oid\":\"5f2bbf61479818904158dbe3\"}",
////			"spider": "qixintong",
////			"desc": "连江县陌上贸易有限公司成立于2020年3月23日，法定代表人为郑青霞，注册资本为100.0万元,统一社会信用代码为91350122MA33N5WU3D，企业地址位于福建省福州市连江县凤城镇马祖东路3号碧水龙城7号楼1305单元，经营范围包括：互联网零售；农业园艺服务；花卉种植；陶瓷、石材装饰材料零售；纺织品及针织品零售；服装零售；鞋帽零售；化妆品及卫生用品零售；钟表、眼镜零售；箱包零售；厨具卫具零售；日用杂品零售；自行车等代步设备零售；文具用品零售；体育用品及器材零售（不含弩）；户外装备零售；珠宝首饰零售；工艺美术品及收藏品零售（象牙及其制品除外）；乐器零售；玩具专门零售；日用家电零售；其他电子产品零售；五金零售；灯具零售；通信设备（不含无线发射装置及卫星地面接收设施）零售。（依法须经批准的项目，经相关部门批准后方可开展经营活动）。连江县陌上贸易有限公司目前的经营状态为存续（在营、开业、在册）。"
////		}
//
//
//		import scala.collection.JavaConversions._
//
//		/**
//		 * 第二步：聚合
//		 * 对entId进行聚合，然后遍历迭代器，将同一企业在不同表的JSON数据（需去重）放入JSONArray中
//		 * 再根据表名为key，JSONArray为值，构建map集合
//		 * 最后map转成一个大的JSONObject
//		 * 格式：{"ent": [{"entId": "5eaa3d2d581f7afded866558",...}，...}]，"ent_recruit": [{"entId": "5eaa3d2d581f7afded866558",...}],...}
//		 */
//		val jsonRDD: RDD[JSONObject] = tupleRDD.groupByKey().map(t => {
//			var set = new util.HashSet[String]()
//			var map = new util.HashMap[String, JSONArray]()
//
//			for (json <- t._2) {
//				val tableName = json.getString("tableName")
//				if (map.get(tableName) == null) {
//					val array = new JSONArray()
//					array.add(json)
//					set.add(json.toJSONString)
//					map.put(tableName, array)
//				} else {
//					val array = map.get(tableName)
//					if (!set.contains(json.toJSONString)) { //去重
//						array.add(json)
//						set.add(json.toJSONString)
//					}
//					map.put(tableName, array)
//				}
//			}
//
//			var finalJson = new JSONObject();
//			for (key <- map.keySet()) {
//				finalJson.put(key, map.get(key))
//			}
//			finalJson
//		}).filter(_.getJSONArray("ent") != null) // 如果其它表有数据，但该企业在Ent表没有数据，则过滤
//
////		jsonRDD.collect().foreach(a=>println(a.toString))
////		{"ent":[{"regNo":"371724600487014","estDate":"2014-08-11","opScope":"服装来料加工、销售","dom":"柳林镇前郝村","entName":"巨野县柳林镇万兴服饰","hidden":"0","historyName":",","source":"百度企业信用","tableName":"ent","legalName":"孔凡连","sourceUrl":"https://aiqicha.baidu.com/company_detail_12967111686228","createdAt":"2020-08-06 15:09:43","regOrgCn":"巨野县市场监督管理局","opFrom":"长期有效","updatedAt":"2020-11-22 04:05:39","entId":"5f2bacb7f4e099f30bc0f2bb","regCapCurCN":"人民币","regProv":"山东省","apprDate":"2014-08-11","corpStatusString":"存续（在营、开业、在册）","regDistrict":"巨野县","q":"91","checkedAt":"2020-09-28 15:52:59","regCity":"菏泽市","entTypeCN":"个体工商户","systemTags":"存续,制造业,纺织服装、服饰业,个体户,纺织服装、服饰业","location":"{\"coordinates\":[115.848434,35.225924],\"type\":\"Point\"}","_id":"{\"$oid\":\"5f2bacb7f4e099f30bc0f2bb\"}","category":"纺织服装、服饰业","spider":"xinbdb","desc":"巨野县柳林镇万兴服饰成立于2014年8月11日，法定代表人为孔凡连，企业地址位于柳林镇前郝村，经营范围包括：服装来料加工、销售。巨野县柳林镇万兴服饰目前的经营状态为存续（在营、开业、在册）。"}]}
////
////		{
////			"ent": [
////			{
////				"regNo": "371724600487014",
////				"estDate": "2014-08-11",
////				"opScope": "服装来料加工、销售",
////				"dom": "柳林镇前郝村",
////				"entName": "巨野县柳林镇万兴服饰",
////				"hidden": "0",
////				"historyName": ",",
////				"source": "百度企业信用",
////				"tableName": "ent",
////				"legalName": "孔凡连",
////				"sourceUrl": "https://aiqicha.baidu.com/company_detail_12967111686228",
////				"createdAt": "2020-08-06 15:09:43",
////				"regOrgCn": "巨野县市场监督管理局",
////				"opFrom": "长期有效",
////				"updatedAt": "2020-11-22 04:05:39",
////				"entId": "5f2bacb7f4e099f30bc0f2bb",
////				"regCapCurCN": "人民币",
////				"regProv": "山东省",
////				"apprDate": "2014-08-11",
////				"corpStatusString": "存续（在营、开业、在册）",
////				"regDistrict": "巨野县",
////				"q": "91",
////				"checkedAt": "2020-09-28 15:52:59",
////				"regCity": "菏泽市",
////				"entTypeCN": "个体工商户",
////				"systemTags": "存续,制造业,纺织服装、服饰业,个体户,纺织服装、服饰业",
////				"location": "{\"coordinates\":[115.848434,35.225924],\"type\":\"Point\"}",
////				"_id": "{\"$oid\":\"5f2bacb7f4e099f30bc0f2bb\"}",
////				"category": "纺织服装、服饰业",
////				"spider": "xinbdb",
////				"desc": "巨野县柳林镇万兴服饰成立于2014年8月11日，法定代表人为孔凡连，企业地址位于柳林镇前郝村，经营范围包括：服装来料加工、销售。巨野县柳林镇万兴服饰目前的经营状态为存续（在营、开业、在册）。"
////			}
////			]
////		}
//
//		//
//		/**
//		 * 第三步：数据清洗
//		 */
////		val mongoRDD: RDD[JSONObject] = jsonRDD
////			.map(MangoHandle.merge)
////			.map(MangoHandle.customCategory(_, bc_categoryList.value))
////			.map(MangoHandle.products)
////			.map(MangoHandle.corpStatusString)
////			.map(MangoHandle.agentType)
////			.map(MangoHandle.check)
////			.map(MangoHandle.setMangoId)
////			.persist(StorageLevel.MEMORY_AND_DISK)
//
////		val esRDD: RDD[JSONObject] = jsonRDD.map(EsHandle.transforToEs)
////
////		// 往es写数据
////		esRDD.map(json => {
////			for (key <- json.keySet()) {
////				if ("ent".equals(key) ||
////					"ent_a_taxpayer".equals(key) ||
////					"ent_abnormal_opt".equals(key) ||
////					"ent_apps".equals(key) ||
////					"ent_bids".equals(key) ||
////					"ent_brand".equals(key) ||
////					"ent_cert".equals(key) ||
////					"ent_contacts".equals(key) ||
////					"ent_copyrights".equals(key) ||
////					"ent_court_notice".equals(key) ||
////					"ent_ecommerce".equals(key) ||
////					"ent_funding_event".equals(key) ||
////					"ent_goods".equals(key) ||
////					"ent_licence".equals(key) ||
////					"ent_new_media".equals(key) ||
////					"ent_news".equals(key) ||
////					"ent_patent".equals(key) ||
////					"ent_punishment".equals(key) ||
////					"ent_recruit".equals(key) ||
////					"ent_software".equals(key) ||
////					"ent_trademark".equals(key) ||
////					"ent_website".equals(key)
////				) {
////					ElasticSearchUtil.postBatchByArray(key, json.getJSONArray(key))
////					//					ElasticSearchUtil.postBatchByArray("prod_" + key, json.getJSONArray(key))
////				}
////			}
////		}).count()
//
//		// 父子文档
//		//        if (executeCode.charAt(1) == '1') {
//		//            val esRDD: RDD[JSONObject] = jsonRDD.map(EsHandle.transforToEs)
//		//            //            esRDD.collect().foreach(println)
//		//
//		//            esRDD.map(json => {
//		//                //先创建空index，并设置mapping
//		//                //                ElasticSearchUtil.createMapping("test")
//		//                /**
//		//                 * 先用HTTP请求，设置mapping
//		//                 * PUT test
//		//                 * {
//		//                 * "mappings": {
//		//                 * "properties": {
//		//                 * "parent_child": {
//		//                 * "type": "join",
//		//                 * "relations": {
//		//                 * "parent": "child"
//		//                 * }
//		//                 * }
//		//                 * }
//		//                 * }
//		//                 * }
//		//                 */
//		//                //插入父文档
//		//                ElasticSearchUtil.postParentDocument("test", json.getJSONObject("ent"))
//		//                import scala.collection.JavaConversions._
//		//                val tablenames = json.keySet()
//		//                //插入子文档
//		//                for (key <- tablenames) {
//		//                    if ("ent_contacts".equals(key) ||
//		//                        "ent_website".equals(key) ||
//		//                        "ent_promotion".equals(key) ||
//		//                        "ent_ecommerce".equals(key) ||
//		//                        "ent_abnormal_opt".equals(key) ||
//		//                        "ent_brand".equals(key) ||
//		//                        "ent_patent".equals(key) ||
//		//                        "ent_funding_event".equals(key)) {
//		//                        ElasticSearchUtil.postChildDocumentByJson("test", json.getJSONObject(key))
//		//
//		//                    } else if ("ent_recruit".equals(key) ||
//		//                        "ent_cert".equals(key) ||
//		//                        "ent_news".equals(key) ||
//		//                        "ent_software".equals(key) ||
//		//                        "ent_licence".equals(key) ||
//		//                        "ent_ecommerce".equals(key) ||
//		//                        "ent_new_media".equals(key) ||
//		//                        "ent_goods".equals(key) ||
//		//                        "ent_lawsuit".equals(key) ||
//		//                        "ent_trademark".equals(key)) {
//		//                        ElasticSearchUtil.postChildDocumentByArray("test", json.getJSONArray(key))
//		//                    }
//		//                }
//		//            }).count()
//		//        }
//
//		sc.stop()
//	}
//}