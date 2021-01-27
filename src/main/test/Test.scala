import com.alibaba.fastjson.{JSON, JSONObject}

import java.text.SimpleDateFormat
import java.util.Date

object Test {
  def main(args: Array[String]): Unit = {
    val line = "ent|{\"_id\":{\"$oid\":\"5f856f284b69cf61f5e274fe\"},\"entName\":\"深圳市康乐通信市场三立通信行\",\"corpStatusString\":\"注销\",\"estDate\":\"2005-04-12\",\"apprDate\":\"2005-04-12\",\"regNo\":\"440323320020689\",\"legalName\":\"孙宏林\",\"opFrom\":\"2005-04-12\",\"opTo\":\"2006-04-12\",\"regProv\":\"广东省\",\"regCity\":\"深圳市\",\"opScope\":\"通讯配件。\",\"dom\":\"深圳市华强北路康乐通信市场3106号\",\"regDistrict\":\"广东省深圳市\",\"source\":\"百度企业信用\",\"spider\":\"xinbdb\",\"sourceUrl\":\"https://aiqicha.baidu.com/company_detail_20570151245410\",\"historyName\":\",\",\"category\":\"计算机、通信和其他电子设备制造业\",\"q\":91,\"hidden\":0,\"createdAt\":1602580264.123,\"entId\":\"5f856f284b69cf61f5e274fe\",\"desc\":\"深圳市康乐通信市场三立通信行成立于2005年4月12日，法定代表人为孙宏林，企业地址位于深圳市华强北路康乐通信市场3106号，经营范围包括：通讯配件。深圳市康乐通信市场三立通信行目前的经营状态为注销。\",\"systemTags\":\"注销,制造业,计算机、通信和其他电子设备制造业,计算机、通信和其他电子设备制造业\"}"
    val content: String = line.substring(line.indexOf("{"), line.length)
    println(content)
    val njson: JSONObject = JSON.parseObject(content)
    val dateStr: String = njson.getInteger("createdAt").toString
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date: String = format.format(new Date(java.lang.Long.parseLong(dateStr) * 1000))
    println(date)


    val regex = "[0-9]{4}-[0-9]{2}-[0-9]{2}"
    val s = "2002-09-0"
    println(s.matches(regex))

    val jsonStr = "{\"_id\":{\"$oid\":\"5f856f284b69cf61f5e274fe\"},\"entName\":\"深圳市康乐通信市场三立通信行\",\"corpStatusString\":\"注销\",\"estDate\":\"2005-04-12\",\"apprDate\":\"2005-04-12\",\"regNo\":\"440323320020689\",\"legalName\":\"孙宏林\",\"opFrom\":\"2005-04-12\",\"opTo\":\"2006-04-12\",\"regProv\":\"广东省\",\"regCity\":\"深圳市\",\"opScope\":\"通讯配件。\",\"dom\":\"深圳市华强北路康乐通信市场3106号\",\"regDistrict\":\"广东省深圳市\",\"source\":\"百度企业信用\",\"spider\":\"xinbdb\",\"sourceUrl\":\"https://aiqicha.baidu.com/company_detail_20570151245410\",\"historyName\":\",\",\"category\":\"计算机、通信和其他电子设备制造业\",\"q\":91,\"hidden\":0,\"createdAt\":1602580264.123,\"entId\":null,\"desc\":\"深圳市康乐通信市场三立通信行成立于2005年4月12日，法定代表人为孙宏林，企业地址位于深圳市华强北路康乐通信市场3106号，经营范围包括：通讯配件。深圳市康乐通信市场三立通信行目前的经营状态为注销。\",\"systemTags\":\"注销,制造业,计算机、通信和其他电子设备制造业,计算机、通信和其他电子设备制造业\"}"
    val jsonObj: JSONObject = JSON.parseObject(jsonStr)
    val entId = jsonObj.getString("entId")
    println(entId)

    val  tableNametuples = Array(("a",1),("a",2),("a",3),("b",2),("b",2),("c",2))
    tableNametuples.groupBy(a=>a._1).foreach(a=>println(a._1+" "+a._2.mkString(",")))


    val list = List(1,2,3,4,5)
    val str = "1,2,3,4,5"
    println(list.mkString(",").equals(str))//true

  }

}
