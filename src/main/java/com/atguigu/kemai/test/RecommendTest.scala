package com.atguigu.kemai.test

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONArray, JSONException, JSONObject}
import com.atguigu.kemai.mango.{MangoHandle, MongoUtils}
import com.atguigu.kemai.recommend.{GetCategory, RecommendUtil}
import com.atguigu.kemai.utils.{ConnectionConstant, JSONUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.types.ObjectId

import java.util
import scala.collection.mutable.ArrayBuffer

object RecommendTest {
  def main(args: Array[String]): Unit = {

    // 参数校验
    if (args.length < 1) {
      println(
        """
				  |----------please enter params:----------
				  |- score: 达标分数
                """.stripMargin)
      sys.exit()
    }
    val score = args(0)

    val spark = SparkSession.builder().appName("HandleKemaiTest")
//      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // 广播dirver端的变量
    val bc_score = sc.broadcast(score.toInt)
    val bc_categoryList = sc.broadcast(GetCategory.getFinalCategoryList)

//    val path = ConnectionConstant.HDFS_URL + "/transform/" + "2021/01/07/"//本地
        val path = ConnectionConstant.HDFS_URL + "/transform/" + "2020/11/03/"//node8

    //只读和推荐维度有关的表
    val fileList = Array(
      path+"ent/*",
      path+"ent_a_taxpayer/*",
      path+"ent_abnormal_opt/*",
      path+"ent_cert/*",
      path+"ent_domain/*",
      path+"ent_growingup/*",
//      path+"ent_listed/*",
      path+"ent_recruit/*",
      path+"ent_top500/*"
    )

    //    val inputDF: DataFrame = spark.read.text(path)
    //    println(inputDF.rdd.getNumPartitions)  //9个分区

    val inputRDD: RDD[String] = sc.textFile(fileList.mkString(","))
    //    println(inputRDD.getNumPartitions)  //60个分区

    //转换成元组 （entId为key）
//    val entIdTupleRDD: RDD[(String, JSONObject)] = inputRDD.map(str => {
//
//      val tableName = str.substring(0,str.indexOf("|"))
//      val jsonStr = str.substring(str.indexOf("|")+1)
//
//      var jsonObj: JSONObject = null
//      var entId:String = null
//      try {
//        jsonObj = JSON.parseObject(jsonStr)
//        jsonObj.put("tableName",tableName)
//        entId = jsonObj.getString("entId")
//        if (entId.equals("empty")) { // 很多entId为empty字符串的值，需过滤，否则该key会造成严重的数据倾斜
//          (null, null)
//        } else {
//          (entId, jsonObj)
//        }
//      }catch{
//        case e:Exception => {
//          println("异常str："+str)
//          println("异常JSON："+jsonStr)
//          e.printStackTrace()
//          (null, null)
//        }
//      }
////      (entId, jsonObj)
//    }).filter(a =>StringUtils.isNotBlank(a._1))

    val entIdTupleRDD: RDD[(String, JSONObject)] = inputRDD.repartition(args(1).toInt).mapPartitions(iter => {

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
          if (entId.equals("empty")) { // 很多entId为empty字符串的值，需过滤，否则该key会造成严重的数据倾斜
            //            list.append((null, null))
          } else {
            list.append((entId, jsonObj))
          }
        }catch{
          case e:Exception => {
            println("异常str："+str)
            println("异常JSON："+jsonStr)
            e.printStackTrace()
            //            list.append((null, null))
          }
        }
      }
      list.iterator
    }).coalesce(args(2).toInt)

    //根据entId分组
    //格式：{"ent": [{"entId": "5eaa3d2d581f7afded866558",...}，...}]，"ent_recruit": [{"entId": "5eaa3d2d581f7afded866558",...}],...}

    // 对样本数据 RDD 统计出每个 key 的出现次数，并按出现次数降序排序。
    // 对降序排序后的数据，取出 top 1 或者 top 100 的数据，也就是 key 最多的前 n 个数据。
    // 具体取出多少个数据量最多的 key，由大家自己决定，我们这里就取 1 个作为示范。
//    val sampleRDD = entIdTupleRDD.sample(false, 0.1)
//    val countMap: collection.Map[String, Long] = sampleRDD.map(a=>(a._1,1)).countByKey()
//    countMap.toList.sortWith(_._2 >_._2).take(20).foreach(println(_))

    val jsonRDD: RDD[JSONObject] = entIdTupleRDD.groupByKey().map { case (entId, iter) => {
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

      resultJsonObj
    }}
      .filter(_.getJSONArray("ent") != null)
//      .filter(_.size()>1)
      .map(MangoHandle.merge)//ent表合并去重
      .map(MangoHandle.customCategory(_, bc_categoryList.value))//行业分类
      .map(MangoHandle.products)//主营产品
      .map(MangoHandle.corpStatusString)//经营状态字符
      .map(MangoHandle.agentType)//机构类型
      .map(MangoHandle.check)//转换部分字段到指定类型
      .map(MangoHandle.setMangoId)//转成mongo的字段类型
      .map(_.getJSONArray("ent").getJSONObject(0))//过滤只留下ent表的第一条数据
      .filter(json => StringUtils.isNotEmpty(json.getString("opScope")))//经营范围不能为空
      .filter(json => {
        val corpStatusString = json.getString("corpStatusString")//经营状态不能为注销
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
//      .coalesce(240, shuffle = true) //缩减分区数，让每个分区数据量相对均匀  todo	一般合并分区数不需要进行shuffle

//    jsonRDD.take(20).foreach(println(_))
//    {"regNo":"610424645004812","entName":"乾县新阳善化寺嫦娥商店","industryCategory":"批发和零售业,零售业","noMobile":1,"noDomain":1,"noTrademark":1,"noEmail":1,"noWechat":1,"legalName":"邢嫦娥","noCourtNotice":1,"uniscId":"92610424MA6XN1XKX1","createdAt":"1603869103","updatedAt":"1603869122","noAnnualReport":1,"isAbnormally":0,"noCr":1,"entId":"5f9919af9c068773428946b8","isTop500":0,"noOnlineService":1,"noAbnomalyOpt":1,"noPatent":1,"regProv":"陕西省","noEquityPledge":1,"apprDate":"2017-05-27","noHttps":1,"corpStatusString":"存续","regDistrict":"乾县","noSr":1,"regCity":"咸阳市","entTypeCN":"个体工商户","noBidding":1,"systemTags":"存续,个体户,批发和零售业,零售业","noWeMedia":1,"isTaxARate":0,"estDate":"2013-11-27","opScope":"烟酒、副食、百货。零售。（依法须经批准的项目，经相关部门批准后方可开展经营活动）","noNews":1,"agentType":"个体户","dom":"陕西省咸阳市乾县新阳乡善化寺村","noPunishment":1,"isGazelle":0,"noJudicialPaper":1,"noICP":1,"regOrgCn":"乾县市场监督管理局","isUnicorn":0,"noTel":1,"noApp":1,"noGoods":1,"noLicence":1,"isHighTech":0,"noBrand":1,"noIllegal":1,"noCert":1,"noWeibo":1,"noInvest":1,"category":"零售业","noFiscalAgent":1,"isStock":0}

    println("--------------------------------------------------jsonRDD--------------------------------------------------")

    //随机key，防止数据倾斜！
    val mongoRDD: RDD[(String, JSONObject)] = jsonRDD.map(RandomTailKeyUtils.randomTailKey)
      .filter(t => StringUtils.isNotEmpty(t._1))

//    todo 抽样检查数据倾斜！
//    val sampleRDD: RDD[(String, JSONObject)] = mongoRDD.sample(false, 0.2)
//    val stringToLong: collection.Map[String, Long] = sampleRDD.countByKey()
//    stringToLong.toList.sortWith(_._2>_._2).take(20).foreach(println(_))


    //根据“行业分类”分组
    // TODO: 这里用flatMap的原因是：要将一行分类的集合转换成一个个的entId元组(entId,List(...))
    import scala.collection.JavaConversions._
    val similarRDD: RDD[(String, util.HashSet[String])] = mongoRDD.groupByKey().flatMap { case (categoryName, iter) => {
      val list = new util.ArrayList[JSONObject]()
      for (jsonObj <- iter) {
        list.add(jsonObj)
      }
//      val list: List[JSONObject] = iter.toList
      // 返回相似企业集合
      val map: util.Map[String, util.HashSet[String]] = RecommendUtil.getRecommend(list, bc_score.value)
      val array = new Array[(String, util.HashSet[String])](map.size());
      var i = 0;
      for (entId <- map.keySet()) {
        array(i) = (entId, map.get(entId))
        i = i + 1
      }
      array
    }}

    similarRDD.take(20).foreach(println(_))
//    (5f9944d97898dbc9e4b571b6,[5f379117f7dabdbbe7e05aee|80.72, 5f47c4e5a4f19087b79fb87c|62.28, 5f105f42b626ead3db8cdedf|60.10, 5f968170f6faf50fcf6e5b0d|67.62, 5f5f6476b4dee77e37c66711|76.72, 5f3ac4c2106be0d39e1094fe|80.72, 5f9e57c8c1c923b4b7bd52a3|65.58, 5f97808c0395147d8173308c|64.87, 5f827ef819abb2a58a46b5d8|70.73, 5f5f36ecbdcdc4ded7a03a05|63.62, 5eaaabcf5095ea63512b4101|61.67, 5f176ff7e09b1a662a681431|61.35, 5f754d22e888bde25fd9321e|70.89, 5f97808c0395147d81733178|69.54, 5f8606744b69cf61f56c2c13|60.15, 5f174bf6c2b8f5e30fca5a7b|62.48, 5eaabbbc5095ea6351a7b118|61.81, 5f90724ea066593ee87d146b|78.16, 5f7f74f5249720f81760fa62|60.95, 5f9eccd7c1c923b4b7e1fbc3|62.97, 5f3c971b13db9b47e51dbef3|65.67, 5f4e5498eaa9032b3d87a58e|64.65, 5fa060b5ff2110bf9ea2cfe3|64.28, 5f754d1ae888bde25fd920cc|60.93, 5f8fa57ca066593ee8f3338f|73.50, 5f73cd30840c5c64dd922621|70.17, 5f9f2c08c1c923b4b705b812|66.00, 5f905625ba5173cc5aaf5ad2|64.28, 5f9a21d3e8ab1a840e712396|62.06, 5f6f81ab76373e7dfc7b2228|60.38, 5f5f951b85aa398ffc56771c|62.91, 5f9468aac60fad139ffda4ec|67.62, 5f527ae7d40b8d45f5e0e83b|67.58, 5eaa937c5095ea635174c551|62.86, 5f2e4d5e31237cf21ac805ad|61.46, 5f84df7c2bd0c2ba9ebdb0be|61.71, 5f8492642bd0c2ba9e618e02|63.25, 5f6094173c3d5c1d55dc0995|66.69, 5f618904a85948987e14ed96|64.28, 5f51ba1e8d7dc15d4695f0c0|64.28, 5f98bffdc5744ec6cc81ed11|75.53, 5f81e5ac19abb2a58a0efb87|63.74, 5f91150ba066593ee806c8e8|80.72, 5f87df1b4b69cf61f5427abb|62.00, 5f5f7589cb8ef5d1a5fafb64|67.86, 5eb83c357b0aa91a6a4fbe2a|74.43, 5f9d7628c1c923b4b76a249b|76.00, 5f73cb4d840c5c64dd8e5b74|62.06, 5f9ad569af37a2cc6da4eb07|74.73, 5f3736cb4a1db0978a4ae76a|63.23, 5f74c724e888bde25fed2496|60.74, 5f5a6ba986ea30601536e37a|63.39, 5f9a4213e8ab1a840e9cea53|61.82, 5f8e7019211f2f1683bbb614|62.86, 5fa0c1b9b460f547c934f013|74.73, 5f6e6b55b9ed2f282a7d9f30|66.59, 5f5fcb7e69b3cd9a707acb46|60.34, 5f739119840c5c64dd191f80|62.49, 5eaa9f1b5095ea6351ca7e76|60.23, 5f1fcf9ce20af37fbd37d0bb|64.94, 5f5a26c0907c93e26f9894cf|64.11, 5f9ece9ac1c923b4b7e277b5|60.05, 5f8f4126a066593ee8be6526|78.16, 5f9d2558c1c923b4b74ca2ff|61.36, 5f47c189836280f8f9f6feec|62.19, 5f903ea5a066593ee85057b4|61.39, 5effd10ac11a8e10808fb729|68.66, 5f5f889abdcdc4ded7a09731|62.05, 5fa0fb99b460f547c95540c5|66.02, 5f95a0f1c75bd81b5dba8a1e|62.23, 5f158f4ff5c3de0b33fcea96|65.06, 5f90baa4a066593ee8bbd9ed|78.26, 5f9bb1c8869e3511b1bc93fb|60.69, 5f9ed287c1c923b4b7e38e4a|67.95, 5f9450aec60fad139fe4aa98|61.82, 5f2fb6dd2260f481134e1ce4|60.60, 5f8e1dd6a066593ee820401f|65.88, 5f9dc4d2c1c923b4b7895c98|67.62, 5f8be8bba066593ee880be43|61.88, 5f9102b6a066593ee8f7d27e|68.27, 5f3b6fb034bc97f312defd6c|61.58, 5f8b9b5aa066593ee82dbe4b|62.02, 5f9ed1cfc1c923b4b7e35baa|80.72, 5f887306e41cea5bcc0c5f01|69.81, 5f3a69abeabd2d8c83b78b8e|67.62, 5f14215a0560a55e1f2203ca|70.35, 5f8b975ba066593ee8293f7c|63.62, 5f9ef622c1c923b4b7f1717a|74.73, 5f9d0570c1c923b4b741b201|64.28, 5f4033915e38172bfc6337fa|63.62, 5f9ab99daf37a2cc6d98caa1|76.00, 5f44c843bcda76d94f473af0|64.54, 5f8f95b7a066593ee8e96580|63.39, 5f74d5a2e888bde25f0c95f9|62.24, 5f5f6f5f907c93e26f9e5f4d|76.72, 5f151a2b50d524469e8becaa|64.34, 5f51dec70f64aaec78e0001e|65.03, 5f9a4234e8ab1a840e9d0386|70.16, 5f9c1a3bc1c923b4b7cf15d5|80.72, 5f2d6c73e611317c1346517c|67.26, 5f9b395aaf37a2cc6deae313|80.72, 5f8258c419abb2a58ae03620|66.51, 5ebbbbc97b0aa91a6aabd259|60.46, 5f7549c9e888bde25fd6594d|63.35, 5f5f64ba189b0724b3137b9e|64.37, 5f627a85f37e92cd6905cbe4|64.28, 5f912892a066593ee81281da|74.73, 5f81b39019abb2a58ac993ea|60.28, 5f6e80d2b9ed2f282a95f91e|62.96, 5f9bb232869e3511b1bca9d7|66.00, 5f9ecc92c1c923b4b7e1e9d1|80.72, 5f9afd32af37a2cc6db42596|64.28, 5f610f8ab6cd0f69b3bfb60e|66.12, 5f8ee724a066593ee88fea16|60.72, 5f94378cc60fad139fb55c4a|71.58, 5f9c1597c1c923b4b7c7e419|80.72, 5f8de037a066593ee8fb9494|62.31, 5f66be2bb8615384b45c5a69|86.00, 5f2e52f66cfa21d72873f4cd|61.82, 5f6cc70fa0a55980d670225e|62.62, 5f9b2dd0af37a2cc6ddc612d|80.72, 5f88163de41cea5bccaec18d|60.98, 5f36a2f8ef717606d988bcc3|74.73, 5f32625993a4d36813059cd6|64.89, 5f105732a212627b978e52e6|61.82, 5fa0b119b460f547c92cae0c|64.28, 5f51e44a7619763d55547dc6|62.68, 5f9478bec60fad139f0ea158|67.62, 5f5f76f7b45e0e4022249757|61.11, 5f9a82a5e8ab1a840ee2a34c|69.81, 5f8e1320a066593ee8198e09|64.79, 5f65b5ce1698982995ed4a66|62.34, 5f8e2389a066593ee823adc0|60.46, 5f618a5d7579ec19dd28787c|60.81, 5f88a26be41cea5bcc3cb99f|62.97, 5f830d4e19abb2a58a30de88|63.67, 5f23bc50035460236944f26d|63.04, 5f76005ae888bde25f0303d0|62.31, 5f7581d7e888bde25f0d56d5|61.81, 5f78d6f59ce7d8b964be17ae|60.20, 5f243b4adccc8c865401d0f1|62.39, 5f2e1fabdf3f2377bc77d4d3|61.67, 5eaa9d8a5095ea6351bb3afe|61.58, 5eaa81975095ea6351c9f5d7|63.62, 5f82702919abb2a58a1eddc8|66.27, 5f5f69f8948929ee83c10ec8|68.66, 5f96ff8c6265768710fa632a|67.19, 5f7c8141a69ef3c5d3c994e4|61.67, 5f9ae2b5af37a2cc6daa1580|76.72, 5f9437abc60fad139fb5a368|65.21, 5efea5de0e8f1febfd980957|63.87, 5f7ef8c4249720f81734cbdc|65.18, 5f8048aa80ae0ced08f0730a|61.12, 5f65b57682d51c46e2e39848|66.68, 5f13464a991a8932c29f608e|80.72, 5f9203a40901c09d479cb1e6|61.82, 5f8e5811a066593ee840a80a|70.73, 5f2ea917cc05d0296f3e7d1f|62.40, 5f6290c938e7051a90a1590e|76.72, 5f7b44c59f381f10feebf99e|66.63, 5f845a552bd0c2ba9e17b4d2|64.28, 5f90448ba066593ee8557184|64.28, 5fa0c126b460f547c934ac7d|60.75, 5fa06716ff2110bf9ea631a9|80.72, 5f8e97eaa066593ee85a09d9|60.11, 5f6a384c3f48bb500ce66c38|64.73, 5f9ecddcc1c923b4b7e24216|86.00, 5f413c54365cde4ea873b5c6|69.73, 5f82eea519abb2a58af23a17|70.17, 5f9eca6fc1c923b4b7e13d98|67.62, 5f124bc5da7b2722282cb841|63.62, 5f9567a0608cb4bd6b79f2c9|62.09, 5eaab76e5095ea63518810ba|76.72, 5f7d0482a69ef3c5d3d1792a|61.89, 5f9ece78c1c923b4b7e26ead|63.60, 5f1bd3ad11685c7bf23f120d|62.62, 5f90c12159e57a5d8187a12a|61.82, 5f5c37a6c34947401fb72b5d|65.37, 5f38a25dfdb34684bec8e8b7|65.66, 5f9ed281c1c923b4b7e38d72|61.82, 5f98c6f6c5744ec6cc8ddc81|61.82, 5f4d7a58de6d10d71e9969fc|80.72, 5f9b14faaf37a2cc6dc4b17f|78.16, 5eaacd0f5095ea63512bb350|62.70, 5f8633f48034b3c2d7b08ee2|62.62, 5f9405b9c60fad139f49870b|60.60, 5f8c3ff1a066593ee8d0fd15|71.21, 5f7578abe888bde25f02e1d3|61.28, 5f69ca1b538a33df83f71d9d|61.40, 5f9f2883c1c923b4b704590a|80.72, 5f962c3290ec84a703467d85|86.00, 5f5a7c04ddd11ff9c62c51f6|64.94, 5f98dbe3c5744ec6ccadc4fd|67.62, 5f3e86f1c9fb096a7531fd1c|62.88, 5f902e81a066593ee844c788|60.81, 5f572b2ce15c14d390c84670|63.62, 5f336983eb7289779ab4c95a|60.28, 5f8b3b3ea066593ee8ea7cbc|60.05, 5f609415d49e8e8904ca7654|69.82, 5f9d060fc1c923b4b741e475|86.00, 5f9ed501c1c923b4b7e4c883|61.30, 5f576f6527e0a3555a171443|62.00, 5f2a0906307c148fa6398ce5|70.92, 5f9e12f1c1c923b4b7a882e6|80.72, 5f2f303b78871dc1771288d3|61.42, 5f4d33d575cc1badd36d4ba1|61.82, 5f7f47d2249720f817c7bc3a|64.82, 5f4e9f40b12be438f2835c87|61.24, 5f61d28e31ed3b80551fd37d|66.00, 5f6f736e76373e7dfc5464b0|63.79, 5f481687e6f09282daa05497|65.73, 5f8cd893a066593ee860da4d|66.75, 5f78d2189ce7d8b964bb07a3|67.90, 5f96672a85dd01fecc821fd2|76.72, 5f88829de41cea5bcc1cc9a7|67.80, 5f7f7122249720f8175c1b8d|68.07, 5eaace925095ea635138130b|60.98, 5f904bfda066593ee85b858e|80.72, 5f9f3b3bc1c923b4b70bc6b9|80.72, 5f2e28333ec3bd0bd60c57d9|60.84, 5f7cc9bda69ef3c5d35490b5|66.24, 5f493963b3dc9a9b0b16b48c|60.18, 5f5f94ed4714f7d65b652c0f|80.72, 5f4114ffa03e733cf47af00f|68.24, 5f611b9fc03c0a99ba392793|80.72, 5f9781350395147d817389b0|62.31, 5f9ecf35c1c923b4b7e2ab70|74.73, 5f9c235ac1c923b4b7de6031|80.72, 5f7f483f249720f817c90f7e|61.18, 5f8719c24b69cf61f5136b19|60.88, 5f77b0d62ee339cb7d17e31a|61.59, 5fa01b25ff2110bf9e8023b2|80.72, 5f900834a066593ee8290265|61.35, 5f7985637b9f5d57007bb8d6|65.81, 5f9cb8a9c1c923b4b72693f6|86.00, 5f95a395c75bd81b5dbd87ae|67.62, 5f82b99819abb2a58a97f761|62.18, 5f9a4f63e8ab1a840eb3527e|69.54, 5f569524380fc5c84862b60d|65.71, 5eb9f0ae7b0aa91a6a6bdaaa|66.67, 5f1498574d6230d1c42432ef|64.21, 5f14411cf360588c914081db|62.70, 5f6f81c876373e7dfc7b95f3|63.57, 5f5f7a185ca9f9f2c7bd60dd|61.82, 5f57a7aadcb4e041126ffbf1|60.70, 5f8cc4f9a066593ee849503a|60.85, 5f884c7ae41cea5bcce55716|62.18, 5fa0285bff2110bf9e86b5b7|80.72, 5f46a7dfa93629c4140a018b|69.08, 5f9e0b8fc1c923b4b7a5c78e|80.72, 5f8e61e0a066593ee8460329|60.85, 5f73c46c840c5c64dd80a1ec|71.15, 5f8a7ad02e3837b9002fde84|62.56, 5f9e01f9c1c923b4b7a24e17|80.72, 5f153a65d6b36a7218fb5d74|64.21, 5ebba34f7b0aa91a6aa5b67e|69.50, 5f73cf6f840c5c64dd95380b|70.67, 5f8efc95a066593ee89b7347|63.39, 5f9c1a9dc1c923b4b7cf8b7c|80.72, 5f9d7736c1c923b4b76a821b|61.07, 5f5eab2cc04e87159fa048c3|67.86, 5f754d36e888bde25fd95f50|62.08, 5ebb3fd27b0aa91a6a84e667|63.80, 5f8f40a8a066593ee8be2196|63.95, 5f9a20c2e8ab1a840e6fb0d4|65.46, 5f8f3c06a066593ee8bc1f08|64.60, 5f156060c948d530252cbab4|68.29, 5f155426a1cf150cfa540cac|62.19, 5f469290aba58a1745f2c165|65.51, 5f82cb0119abb2a58aabccc1|75.39, 5f9edcf3c1c923b4b7e86e85|63.95, 5f64efc52573c36387a79869|64.28, 5f9560c6608cb4bd6b715046|78.16, 5f81b37919abb2a58ac9783f|62.43, 5f5f9474460e9f20cba1f255|63.95, 5f90819ca066593ee88b009a|61.82, 5f474152c6b50df51a17f239|63.16, 5f9800b4c50a61ea41ce9f09|69.52, 5f62d803a96c35462793eb59|86.00, 5f78bbc09ce7d8b9649874c6|64.16, 5f41735d36e4e14db83e1c72|63.10, 5f9494983389179a593e65e8|80.72, 5f79e36576f2f74114dcc789|73.83, 5f961b4b3a676a4d7df84c91|62.31, 5f2e623b6f24cbcd063da0bb|62.88, 5f94ee30db9ee314bfb7f55e|80.72, 5f70da6a05b322c913fe3240|61.02, 5f88a796e41cea5bcc41ccf6|69.54, 5f3fb287369b5cf942f10814|64.07, 5f7ce7e0a69ef3c5d3793495|60.04, 5f9cac5cc1c923b4b721a033|64.28, 5f943b87f0dde7909d579167|70.11, 5f14463b15492f16c1c7bdcc|60.10, 5f81cdf919abb2a58aed91ac|70.33, 5f95679e608cb4bd6b79ef6a|63.16, 5f3bf61b40b504e889026f44|66.08, 5f4740745f67a98b76abcee6|68.51, 5f8b8c1ea066593ee81bf826|62.69, 5f8e09eca066593ee813e4fd|64.44, 5f4a02f388495f8bc7e753dc|67.96, 5f5824cd7f7d745d2e725aaf|69.39, 5f9778830395147d816a6f4a|63.62, 5f8ffcd2a066593ee8228b13|69.81, 5f9ed207c1c923b4b7e36d0c|63.04, 5f6a4b420868c2d9e8da055f|86.00, 5f9a216ce8ab1a840e708897|60.15, 5f2f8814f821ce1c630c00cc|70.43, 5f718eb105b322c91332180d|63.47, 5f91bbfa01ab9da4a18c6647|74.73, 5f9b3acfaf37a2cc6deb9a7b|65.36, 5f9051b2a066593ee86104bb|64.28, 5f422ab8e9b645c8ad9a9786|60.09, 5f3025fefdd37ddd47d96c45|68.27, 5f97f487b66fd5ea91add633|69.54, 5f2f3286c4651748771f9a1f|67.62, 5f759bd6e888bde25f2e9860|63.62, 5f7f80a0249720f8176f4351|69.56, 5f363af149de4c858b1675c5|63.80, 5f691d56b50e733236415ce7|64.90, 5f5f872ca118ac8e07f2a96c|80.72, 5f5597052cc9b5169e5a2e91|61.99, 5f9cab7fc1c923b4b7213e70|64.28])

    // TODO: 这里的代码，最终的相似度应该按照评分从大到小的顺序排列，而不是只是获取到遍历的符合要求的前N个！！
    // TODO: 这里可以用foreachPartition来代替！
//    similarRDD.mapPartitions(similarFunc).count()
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
