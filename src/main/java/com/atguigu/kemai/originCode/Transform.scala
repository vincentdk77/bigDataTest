package com.atguigu.kemai.originCode

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by JarvisKwok
 * Date :2020/10/9
 * Description : 日志转换
 */
object Transform {
	def main(args: Array[String]): Unit = {
		if (args.length != 1) {
			println(
				"""
				  |----------please enter params:----------
				  |- isRerun：是否重新执行之前数据（yes or no）
				  |- date：日志日期
				  |- tableName: 表名
                """.stripMargin)
			sys.exit()
		}
//		var Array(isRerun, date, tableName) = args
		var Array(tableName) = args

//		if (isRerun.equals("no")) {
//			val format = new SimpleDateFormat("yyyy-MM-dd")
//			date = format.format(new Date())
//		}

		val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
		val sc = new SparkContext(conf)

		sc.textFile("hdfs://jtb/mongodata/" + tableName + "/2020-11-21")
			.map(tableName + "|" + _)
			.saveAsTextFile("hdfs://jtb/transform/" + tableName + "/2020-11-21")
		sc.stop()
	}
}
