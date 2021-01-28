package com.atguigu.kemai.test

import com.atguigu.kemai.utils.ConnectionConstant
import org.apache.spark.sql.SparkSession

object TransformTest {
	System.setProperty("HADOOP_USER_NAME","root")
	def main(args: Array[String]): Unit = {
		//args=		ent ent_a_taxpayer ent_abnormal_opt ent_annual_report ent_apps ent_bids ent_brand ent_cert ent_contacts ent_copyrights ent_court_notice ent_court_operator ent_court_paper ent_dishonesty_operator ent_ecommerce ent_equity_pledged ent_funding_event ent_goods ent_invest_company ent_licence  ent_maimai  ent_new_media  ent_news ent_patent ent_punishment ent_recruit ent_software ent_trademark ent_website ent_zhaodao
//		if (args.length < 1) {
//			println("请输入表名，用空格分隔！")
//			sys.exit()
//		}

		val spark = SparkSession.builder()
				.master("local[*]")
				.appName("Transform").getOrCreate()
		val sc = spark.sparkContext

//		if(args!= null && args.length>0){
//			args.foreach(tableName =>{
//				//源数据ent：{"_id":{"$oid":"5f856f0f8034b3c2d705f647"},"uniscId":"91211103MA10E6140W","entTypeCN":"有限责任公司（自然人投资或控股）","entName":"盘锦信朋缘劳务有限公司","corpStatusString":"存续（在营、开业、在册）","estDate":"2020-06-04","apprDate":"2020-06-04","regOrgCn":"盘锦市兴隆台区市场监督管理局","regCapCurCN":"人民币","regCaption":100.0,"regCap":"壹佰万元整","legalName":"张艳阳","opFrom":"2020-06-04","opTo":"2040-06-03","regProv":"辽宁省","regCity":"盘锦市","regDistrict":"兴隆台区","opScope":"许可项目：建筑劳务分包，住宅室内装饰装修，各类工程建设活动，消防设施工程（依法须经批准的项目，经相关部门批准后方可开展经营活动，具体经营项目以审批结果为准） 一般项目：园林绿化工程施工，家政服务，五金产品零售，日用百货销售，家用电器销售，建筑装饰材料销售，机械设备租赁，消防器材销售（除依法须经批准的项目外，凭营业执照依法自主开展经营活动）","dom":"辽宁省盘锦市兴隆台区泰山路东、大众街北王家荒回迁楼3#11003","source":"爱企查公众号","spider":"aiqichaapp","q":91,"hidden":0,"createdAt":1602580239,"entId":"5f856f0f8034b3c2d705f647","desc":"盘锦信朋缘劳务有限公司成立于2020年6月4日，法定代表人为张艳阳，注册资本为100.0万元,统一社会信用代码为91211103MA10E6140W，企业地址位于辽宁省盘锦市兴隆台区泰山路东、大众街北王家荒回迁楼3#11003，经营范围包括：许可项目：建筑劳务分包，住宅室内装饰装修，各类工程建设活动，消防设施工程（依法须经批准的项目，经相关部门批准后方可开展经营活动，具体经营项目以审批结果为准） 一般项目：园林绿化工程施工，家政服务，五金产品零售，日用百货销售，家用电器销售，建筑装饰材料销售，机械设备租赁，消防器材销售（除依法须经批准的项目外，凭营业执照依法自主开展经营活动）。盘锦信朋缘劳务有限公司目前的经营状态为存续（在营、开业、在册）。"}
//				//目标数据ent: ent|{"_id":{"$oid":"5f856f0f8034b3c2d705f647"},"uniscId":"91211103MA10E6140W","entTypeCN":"有限责任公司（自然人投资或控股）","entName":"盘锦信朋缘劳务有限公司","corpStatusString":"存续（在营、开业、在册）","estDate":"2020-06-04","apprDate":"2020-06-04","regOrgCn":"盘锦市兴隆台区市场监督管理局","regCapCurCN":"人民币","regCaption":100.0,"regCap":"壹佰万元整","legalName":"张艳阳","opFrom":"2020-06-04","opTo":"2040-06-03","regProv":"辽宁省","regCity":"盘锦市","regDistrict":"兴隆台区","opScope":"许可项目：建筑劳务分包，住宅室内装饰装修，各类工程建设活动，消防设施工程（依法须经批准的项目，经相关部门批准后方可开展经营活动，具体经营项目以审批结果为准） 一般项目：园林绿化工程施工，家政服务，五金产品零售，日用百货销售，家用电器销售，建筑装饰材料销售，机械设备租赁，消防器材销售（除依法须经批准的项目外，凭营业执照依法自主开展经营活动）","dom":"辽宁省盘锦市兴隆台区泰山路东、大众街北王家荒回迁楼3#11003","source":"爱企查公众号","spider":"aiqichaapp","q":91,"hidden":0,"createdAt":1602580239,"entId":"5f856f0f8034b3c2d705f647","desc":"盘锦信朋缘劳务有限公司成立于2020年6月4日，法定代表人为张艳阳，注册资本为100.0万元,统一社会信用代码为91211103MA10E6140W，企业地址位于辽宁省盘锦市兴隆台区泰山路东、大众街北王家荒回迁楼3#11003，经营范围包括：许可项目：建筑劳务分包，住宅室内装饰装修，各类工程建设活动，消防设施工程（依法须经批准的项目，经相关部门批准后方可开展经营活动，具体经营项目以审批结果为准） 一般项目：园林绿化工程施工，家政服务，五金产品零售，日用百货销售，家用电器销售，建筑装饰材料销售，机械设备租赁，消防器材销售（除依法须经批准的项目外，凭营业执照依法自主开展经营活动）。盘锦信朋缘劳务有限公司目前的经营状态为存续（在营、开业、在册）。"}
//				println("=========="+tableName+"===========")
////				sc.textFile(ConnectionConstant.HDFS_URL+"/mongodata/"+tableName+".json")
////					.map(tableName + "|" + _)
//					//写完后文件在hdfs上是这样的：_SUCCESS ， part-00000  ，part-00001
//					//文件内容如下：
//					// ent|{"_id":{"$oid":"5f856f0f8034b3c2d705f647"},"uniscId":"91211103MA10E6140W","entTypeCN":"有限责任公司（自然人投资或控股）","entName":"盘锦信朋缘劳务有限公司","corpStatusString":"存续（在营、开业、在册）","estDate":"2020-06-04","apprDate":"2020-06-04","regOrgCn":"盘锦市兴隆台区市场监督管理局","regCapCurCN":"人民币","regCaption":100.0,"regCap":"壹佰万元整","legalName":"张艳阳","opFrom":"2020-06-04","opTo":"2040-06-03","regProv":"辽宁省","regCity":"盘锦市","regDistrict":"兴隆台区","opScope":"许可项目：建筑劳务分包，住宅室内装饰装修，各类工程建设活动，消防设施工程（依法须经批准的项目，经相关部门批准后方可开展经营活动，具体经营项目以审批结果为准） 一般项目：园林绿化工程施工，家政服务，五金产品零售，日用百货销售，家用电器销售，建筑装饰材料销售，机械设备租赁，消防器材销售（除依法须经批准的项目外，凭营业执照依法自主开展经营活动）","dom":"辽宁省盘锦市兴隆台区泰山路东、大众街北王家荒回迁楼3#11003","source":"爱企查公众号","spider":"aiqichaapp","q":91,"hidden":0,"createdAt":1602580239,"entId":"5f856f0f8034b3c2d705f647","desc":"盘锦信朋缘劳务有限公司成立于2020年6月4日，法定代表人为张艳阳，注册资本为100.0万元,统一社会信用代码为91211103MA10E6140W，企业地址位于辽宁省盘锦市兴隆台区泰山路东、大众街北王家荒回迁楼3#11003，经营范围包括：许可项目：建筑劳务分包，住宅室内装饰装修，各类工程建设活动，消防设施工程（依法须经批准的项目，经相关部门批准后方可开展经营活动，具体经营项目以审批结果为准） 一般项目：园林绿化工程施工，家政服务，五金产品零售，日用百货销售，家用电器销售，建筑装饰材料销售，机械设备租赁，消防器材销售（除依法须经批准的项目外，凭营业执照依法自主开展经营活动）。盘锦信朋缘劳务有限公司目前的经营状态为存续（在营、开业、在册）。"}
//					.take(20).foreach(a =>println(a.toString()))
////					.saveAsTextFile(ConnectionConstant.HDFS_URL+"/transform/"+"2020/11/03/"+ tableName)
//			})
//		}

		val inputRDD  = sc.textFile(ConnectionConstant.HDFS_URL+"/mongodata/ent_c*.json")//一行数据构成一个RDD的元素
		println("inputRDD元素个数："+inputRDD.count())//300000
		println(inputRDD.getNumPartitions)//有30个分区,local模式, todo 为啥不是60个呢？

		val inputWholeRDD = sc.wholeTextFiles(ConnectionConstant.HDFS_URL+"/mongodata")//一个文件构成一个RDD的元素
		println("inputWholeRDD元素个数："+inputWholeRDD.count())//30

		println(inputWholeRDD.getNumPartitions)//只有两个分区,local模式

		val mapRDD = inputWholeRDD.map{case (path,json) =>{
				val tableName = path.substring(path.lastIndexOf("/")+1,path.indexOf("."))
//			println(tableName)
				(tableName,json)
		}}
//			.saveAsTextFile(ConnectionConstant.HDFS_URL+"/transform/"+"2020/11/03/"+ tableName)


		spark.stop()


//		//方法二：用sparksql spark.read.json(path)
//		import spark.implicits._
//		if(args!= null && args.length>0){
//			args.foreach(tableName => {
//				val df = spark.read.json(ConnectionConstant.HDFS_URL+"/mongodata/"+tableName+".json")
//					.map(row=>tableName + "|" + row.toString())
////				df.show(truncate = false)
//				df.rdd.take(20).foreach(println)
//				// 为什么DF写出来的是这样的格式 因为Row的toString方法重写了，格式就是这样
//				//{"value":"ent_a_taxpayer|[A,[5f44d97d4c323186fb817c4e],empty,淮北龙旺实业有限公司,国家税务总局淮北市烈山区税务局,信用安徽,http://credit.ah.gov.cn/remote/1480/index_2.htm,91340600754858206W,2019]"}
////				df.write.json(ConnectionConstant.HDFS_URL+"/transform/" + tableName + "/2021-01-26")
//			})
//		}
//		spark.stop()
	}
}
