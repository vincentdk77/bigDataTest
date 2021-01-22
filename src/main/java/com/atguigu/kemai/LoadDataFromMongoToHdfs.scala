package com.atguigu.kemai

import org.apache.spark.sql.SparkSession


object LoadDataFromMongoToHdfs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.default.parallelism","16")
      .config("spark.sql.shuffle.partitions","16")
      .appName("LoadDataFromMongoToHdfs")
      .getOrCreate()

    val collections = Array(
//      "ent", // ES索引，统计字段
//      "ent_a_taxpayer", // ES索引，统计字段
//      "ent_abnormal_opt", // ES索引
//      "ent_annual_report", // 统计字段
//      "ent_apps", // ES索引，统计字段
//      "ent_bids", // ES索引
//      "ent_brand", // ES索引
//      "ent_cert", // ES索引，统计字段
//      "ent_contacts", // ES索引，统计字段
//      "ent_copyrights", // ES索引
//      "ent_court_notice", // 统计字段
//      "ent_court_operator", // ES索引
//      "ent_court_paper", // 统计字段
//      "ent_dishonesty_operator", //
//      "ent_ecommerce", // ES索引，统计字段
//      "ent_equity_pledged", // 统计字段
//      "ent_funding_event", // ES索引
//      "ent_goods", // ES索引
//      "ent_invest_company", // 统计字段
//      "ent_licence", // ES索引，统计字段
//      "ent_new_media", // ES索引，统计字段
//      "ent_news", // ES索引，统计字段
//      "ent_patent", // ES索引，统计字段
//      "ent_punishment", // ES索引，统计字段
//      "ent_recruit", // ES索引，统计字段
//      "ent_software", // ES索引，统计字段
//      "ent_trademark", // ES索引，统计字段
//      "ent_website", // ES索引，统计字段
//      "ent_maimai", //
      "ent_top500" //
//      "ent_zhaodao" //
    )
    for (tableName <- collections) {
      val mongoDF = spark.read
        .option("uri", "mongodb://hadoop102:27017/ent")
//        .option("uri", "mongodb://192.168.0.81:28018/ent")
//        .option("user", "spiderman")
//        .option("password", "spider\\$2581\\#")
        .option("collection", tableName)
        .format("com.mongodb.spark.sql")
        .load()
//        .mapPartitions(iter=>{
//          val array = ArrayBuffer[String]()
//          for (row <- iter) {
//            val str = row.toString()
//            array.append(str)
////            val tableName = str.substring(1,str.indexOf("|"))//去掉第一个[
////            val jsonStr = str.substring(str.indexOf("|")+1,str.length-1)//去掉最后一个]
//          }
//
//          array.iterator
//        })

//      mongoDF.foreach(ent=>println(ent.entName))
      mongoDF.show(truncate = false)


    }


    spark.close()


  }

}
