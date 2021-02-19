package com.atguigu.kemai.test

import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.StringUtils

object RandomTailKeyUtils {

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
  def randomTailKey(json:JSONObject)={ // TODO: 防止数据倾斜的方法重写一个！
    val industryCategory = json.getString("industryCategory")

    if (StringUtils.isNotEmpty(industryCategory)) {
      val words: Array[String] = industryCategory.split(",")
      (words(0), json)
//
//      if (words(0).equals("电力、热力、燃气及水生产和供应业")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(9) //82917
//        (newKey, json)
//      }
//      else if (words(0).equals("批发和零售业")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(1274) //12731921
//        (newKey, json)
//      }
//      else if (words(0).equals("房地产业")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(72) //719503
//        (newKey, json)
//      }
//      else if (words(0).equals("文化、体育和娱乐业")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(33) //330375
//        (newKey, json)
//      }
//      else if (words(0).equals("水利、环境和公共设施管理业")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(12) //112619
//        (newKey, json)
//      }
//      else if (words(0).equals("卫生和社会工作")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(5) //45808
//        (newKey, json)
//      }
//      else if (words(0).equals("租赁和商务服务业")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(356) //3553630
//        (newKey, json)
//      }
//      else if (words(0).equals("住宿和餐饮业")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(136) //1361901
//        (newKey, json)
//      }
//      else if (words(0).equals("制造业")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(415) //4151372
//        (newKey, json)
//      }
//      else if (words(0).equals("信息传输、软件和信息技术服务业")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(124) //1240645
//        (newKey, json)
//      }
//      else if (words(0).equals("科学研究和技术服务业")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(183) //1832334
//        (newKey, json)
//      }
//      else if (words(0).equals("建筑业")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(192) //1920922
//        (newKey, json)
//      }
//      else if (words(0).equals("交通运输、仓储和邮政业")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(80) //797464
//        (newKey, json)
//      }
//      else if (words(0).equals("农、林、牧、渔业")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(82) //817200
//        (newKey, json)
//      }
//      else if (words(0).equals("金融业")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(28) //281544
//        (newKey, json)
//      }
//      else if (words(0).equals("教育")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(14) //140219
//        (newKey, json)
//      }
//      else if (words(0).equals("采矿业")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(6) //53471
//        (newKey, json)
//      }
//      else if (words(0).equals("居民服务、修理和其他服务业")) {
//        val newKey = words(0) + "_" + scala.util.Random.nextInt(134) //1337286
//        (newKey, json)
//      }
//      else {
//        (words(0), json)
//      }
    } else { // 所有数据必须要有industryCategory字段，否则都是垃圾数据
      (null, null)
    }
  }

}
