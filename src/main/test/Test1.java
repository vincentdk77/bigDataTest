import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Test1 {
    public static void main(String[] args) throws InterruptedException, ParseException {
        List<String> a = ListUtil.toLinkedList("1", "2", "3");
        System.out.println(a);
        List<String> filter = ListUtil.filter(a, str -> "edit" + str);
        System.out.println(filter);

        List<String> b = ListUtil.toLinkedList("1", "2", "3", "4", "3", "2", "1");
        // [1, 5]
        int[] indexArray = ListUtil.indexOfAll(b, "2"::equals);
        for (int i : indexArray) {
            System.out.println(i);
        }

        Date date1 = new Date();
//        Thread.sleep(1000);
        Date date2 = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date startDate = format.parse("1800-01-01");
        System.out.println( DateUtil.compare(date2, startDate)>0 );

        String dateStr = "2017-03-01";
        String dateStr1 = "2017-03-01 21:23";
        String dateStr2 = "2017-03-01 21:23:30";
        Date date = DateUtil.parse(dateStr);
        Date date3 = DateUtil.parse(dateStr1);
        Date date4 = DateUtil.parse(dateStr2);
        System.out.println(date.toString());
        System.out.println(date3);
        System.out.println(date4);

        List<String> c = ListUtil.toLinkedList("1", "2", "3", "4", "3", "2", "1");
        for (int i = c.size()-1 ;i >= 0;  i--) {
            if(c.get(i).equals("2")){
                c.remove(i);
            }
        }
        System.out.println(c);

        String line = "{\"_id\":{\"$oid\":\"5f856f284b69cf61f5e274fe\"},\"entName\":\"深圳市康乐通信市场三立通信行\",\"corpStatusString\":\"注销\",\"estDate\":\"2005-04-12\",\"apprDate\":\"2005-04-12\",\"regNo\":\"440323320020689\",\"legalName\":\"孙宏林\",\"opFrom\":\"2005-04-12\",\"opTo\":\"2006-04-12\",\"regProv\":\"广东省\",\"regCity\":\"深圳市\",\"opScope\":\"通讯配件。\",\"dom\":\"深圳市华强北路康乐通信市场3106号\",\"regDistrict\":\"广东省深圳市\",\"source\":\"百度企业信用\",\"spider\":\"xinbdb\",\"sourceUrl\":\"https://aiqicha.baidu.com/company_detail_20570151245410\",\"historyName\":\",\",\"category\":\"计算机、通信和其他电子设备制造业\",\"q\":91,\"hidden\":0,\"createdAt\":1602580264.123,\"entId\":null,\"desc\":\"深圳市康乐通信市场三立通信行成立于2005年4月12日，法定代表人为孙宏林，企业地址位于深圳市华强北路康乐通信市场3106号，经营范围包括：通讯配件。深圳市康乐通信市场三立通信行目前的经营状态为注销。\",\"systemTags\":\"注销,制造业,计算机、通信和其他电子设备制造业,计算机、通信和其他电子设备制造业\"}";
        JSONObject jsonObj = JSONObject.parseObject(line);
        Iterator<Map.Entry<String, Object>> iterator = jsonObj.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            String key = next.getKey();
            Object value = next.getValue();
            if(key.endsWith("Date")){
                jsonObj.put(key, "##################");
            }else{
                System.out.println("移除！");
                iterator.remove();
            }
        }
        System.out.println(jsonObj);

    }
}
