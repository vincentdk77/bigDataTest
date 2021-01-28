package com.atguigu.kemai.utils;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class JSONUtils {

    public static void main(String agrs[]) {
        String s = "{\"entName\": \"\", \"anCheYear\": \"\", \"anCheId\": \"F4FDD211FAE01BE337CC0FCD19E23DAB61FD91FD91FD9117468C325EB1E0B1458F5B3766D812C6AAFB977818D129F12549A649F7CA74-1590205236011\", \"uniscId\": \"\", \"entType\": \"1\", \"nodeNum\": \"370000\", \"raw\": \"{\\\"result\\\":{\\\"eGrpName\\\":null,\\\"eGrpShform\\\":null,\\\"ePubGroup\\\":null,\\\"elnfyreturnbuild\\\":null,\\\"isPublicPeriod\\\":null,\\\"simpleCanrea\\\":null,\\\"noticeFrom\\\":null,\\\"noticeTo\\\":null,\\\"pripId\\\":\\\"4B466DAA455BA4588877B076A6598210DE462E462E46881078C20C640C648FE70C680068B45F37DC89E15B871FA5-1590205308722\\\",\\\"regNo\\\":\\\"370830200002833\\\",\\\"uniscId\\\":\\\"91370830554391306G\\\",\\\"regState\\\":\\\"1\\\",\\\"regState_CN\\\":\\\"在营（开业）企业\\\",\\\"entType\\\":\\\"1\\\",\\\"name\\\":\\\"贾正国\\\",\\\"industryPhy\\\":\\\"C\\\",\\\"entTypeForQuery\\\":1,\\\"entTypeForAnnualReport\\\":1,\\\"linkmanName\\\":\\\"\\\",\\\"linkmanCerNo\\\":\\\"\\\",\\\"linkmanPhone\\\":\\\"\\\",\\\"busExceptCount\\\":0,\\\"illCount\\\":0,\\\"nodeNum\\\":\\\"370000\\\",\\\"entName\\\":\\\"汶上县金秋食品有限公司\\\",\\\"entType_CN\\\":\\\"有限责任公司(自然人投资或控股)\\\",\\\"regCap\\\":1001.0,\\\"regCapStr\\\":null,\\\"dom\\\":\\\"汶上县次丘镇河里村\\\",\\\"opFrom\\\":\\\"2010-04-30\\\",\\\"opTo\\\":null,\\\"opScope\\\":\\\"肉食蛋类的生产、销售。（涉及许可经营的须凭许可证或批准文件经营）(有效期限以许可证为准)。(依法须经批准的项目，经相关部门批准后方可开展经营活动)。\\\",\\\"regOrg_CN\\\":\\\"汶上县市场监督管理局\\\",\\\"regOrg\\\":\\\"370830\\\",\\\"estDate\\\":\\\"2010-04-30\\\",\\\"apprDate\\\":\\\"2020-02-26\\\",\\\"revDate\\\":null,\\\"regCapCur_CN\\\":\\\"人民币\\\"},\\\"anCheYear\\\":\\\"2013\\\",\\\"nodeNum\\\":\\\"370000\\\",\\\"annRep\\\":{\\\"annRepDataAlterstock\\\":[],\\\"isInvest\\\":\\\"否\\\",\\\"annRepDataInvestment\\\":[],\\\"annRepDataWebsite\\\":[],\\\"annRepDataSocsecinfo\\\":[],\\\"annRepDataAlt\\\":[{\\\"altId\\\":\\\"09E73980C7D1219EE05012AC9E016078\\\",\\\"anCheId\\\":\\\"045380155B3E0583E05012AC9E011868\\\",\\\"alitem\\\":\\\"股东及出资信息\\\",\\\"altBe\\\":\\\"投资人: 贾正国累计认缴额: 50认缴出资日期: 2010-04-30认缴出资方式: 1|货币 累计实缴额: 50实缴出资日期: 2010-04-30实缴出资方式: 1|货币\\\",\\\"altAf\\\":\\\"该股东及出资信息已经删除\\\",\\\"altDate\\\":\\\"2014-12-11\\\",\\\"lastModifiedTime\\\":null,\\\"lastModifiedPerson\\\":\\\"\\\",\\\"sExtSequence\\\":\\\"2308294c8461ae19d367519fce27c091\\\"}],\\\"annRepDataSponsor\\\":[{\\\"invId\\\":\\\"f6ebcc1f30b244c0a3949156b5b30dce\\\",\\\"anCheId\\\":\\\"045380155B3E0583E05012AC9E011868\\\",\\\"invName\\\":\\\"贾正国\\\",\\\"liSubConAm\\\":50.0,\\\"subConDate\\\":\\\"2010-04-30\\\",\\\"subConForm\\\":\\\"\\\",\\\"subConFormName\\\":\\\"1\\\",\\\"liAcConAm\\\":50.0,\\\"acConDate\\\":\\\"2010-04-30\\\",\\\"acConForm\\\":\\\"\\\",\\\"acConForm_CN\\\":\\\"1\\\",\\\"lastModifiedPerson\\\":\\\"\\\",\\\"lastModifiedTime\\\":null,\\\"sExtSequence\\\":\\\"d341e1d39381bb38ad9b672d17b5418b\\\",\\\"vAnnualReportAlters\\\":null}],\\\"annRepDataGuaranteeinfo\\\":[],\\\"isWeb\\\":\\\"否\\\",\\\"isMorg\\\":\\\"否\\\",\\\"annRepData\\\":{\\\"pripId\\\":\\\"370830001012009101508978\\\",\\\"anCheId\\\":\\\"045380155B3E0583E05012AC9E011868\\\",\\\"anCheDate\\\":\\\"2014-12-11\\\",\\\"anCheYear\\\":\\\"2013\\\",\\\"entType\\\":\\\"1130\\\",\\\"regNo\\\":\\\"370830200002833\\\",\\\"uniscId\\\":\\\"91370830554391306G\\\",\\\"entName\\\":\\\"汶上县金秋食品有限公司\\\",\\\"tel\\\":\\\"0537-7076312\\\",\\\"addr\\\":\\\"汶上县次丘镇河里村\\\",\\\"postalCode\\\":\\\"272500\\\",\\\"email\\\":\\\"LXP19812004@126.com\\\",\\\"busSt\\\":\\\"1\\\",\\\"busSt_CN\\\":\\\"开业\\\",\\\"empNum\\\":5,\\\"empNumDis\\\":\\\"2\\\",\\\"colGraNum\\\":0,\\\"colEmplNum\\\":0,\\\"retSolNum\\\":0,\\\"retEmplNum\\\":0,\\\"disPerNum\\\":0,\\\"disEmplNum\\\":0,\\\"uneNum\\\":0,\\\"uneEmplNum\\\":0,\\\"assGro\\\":71.33,\\\"assGroDis\\\":\\\"2\\\",\\\"liaGro\\\":5.04,\\\"liaGroDis\\\":\\\"2\\\",\\\"vendInc\\\":32.87,\\\"vendIncDis\\\":\\\"2\\\",\\\"maiBusInc\\\":32.87,\\\"maiBusIncDis\\\":\\\"2\\\",\\\"proGro\\\":-1.02,\\\"proGroDis\\\":\\\"2\\\",\\\"netInc\\\":-1.02,\\\"netIncDis\\\":\\\"2\\\",\\\"ratGro\\\":0.49,\\\"ratGroDis\\\":\\\"2\\\",\\\"totEqu\\\":66.29,\\\"totEquDis\\\":\\\"2\\\",\\\"numParM\\\":null,\\\"parIns\\\":\\\"\\\",\\\"parIns_CN\\\":\\\"\\\",\\\"resParMSign\\\":\\\"\\\",\\\"resParSecSign\\\":\\\"\\\",\\\"dependentEntName\\\":\\\"\\\",\\\"lastModifiedTime\\\":null,\\\"isDis\\\":null,\\\"lastModifiedPerson\\\":\\\"\\\",\\\"sExtSequence\\\":\\\"571a1bdd81b1aa38e73c38025f6714e6\\\",\\\"anType\\\":\\\"11\\\",\\\"dependentName\\\":null,\\\"dependentUniscId\\\":null,\\\"mainBusiAct\\\":null,\\\"womemPNum\\\":null,\\\"womemPNumDis\\\":null,\\\"holdingSmsg\\\":null,\\\"holdingSmsgDis\\\":null,\\\"holdingSmsg_cn\\\":null,\\\"annRepFrom\\\":null,\\\"annBaseUrl\\\":null,\\\"annSfcBaseUrl\\\":null,\\\"annPbBaseUrl\\\":null,\\\"annBaseJsUrl\\\":null,\\\"annSfcBaseJsUrl\\\":null,\\\"annPbBaseJsUrl\\\":null,\\\"sponsorUrl\\\":null,\\\"forInvestmentUrl\\\":null,\\\"forGuaranteeinfoUrl\\\":null,\\\"webSiteInfoUrl\\\":null,\\\"alterUrl\\\":null,\\\"alterStockInfoUrl\\\":null,\\\"branchProductionUrl\\\":null,\\\"annSocsecinfoUrl\\\":null,\\\"baseInfoUrl\\\":null,\\\"alterTotalPageUrl\\\":null,\\\"vAnnualReportSfcBranchUrl\\\":null,\\\"annulLicenceUrl\\\":null,\\\"vannualSfcAssertUrl\\\":null,\\\"annSfccSocsecinfoUrl\\\":null,\\\"vAnnPbAssetUrl\\\":null,\\\"annRepDetailUrl\\\":null,\\\"dis\\\":null,\\\"vAnnualReportAlters\\\":null},\\\"isStockAlter\\\":\\\"否\\\"},\\\"anCheId\\\":\\\"F0FDD611FEE01FE333CC0BCD1DE239AB65FD95FD95FD9517428C365EB5E0B5458B5B3366DC12C2AAFF977C18D529F5254DA64DF7CE74-1590205308722\\\",\\\"entTypeForQuery\\\":1}\", \"createdAt\": 1590205862.9546251}";
        System.out.println(getNotNullJson(s));

    }

    public static JSONObject getJSONObject(String sign, String customerIds) {
        JSONObject json = new JSONObject();
        json.put(sign, customerIds);
        return json;
    }

    public static JSONObject getJson(String line) {
        JSONObject json = new JSONObject();
        try {
            json = JSONObject.parseObject(line);
        } catch (Exception e) {
            System.out.println("line:" + line);
            e.printStackTrace();

        }
        return json;
    }

    /**
     * 解析json字符串成jsonObject，并针对“各种日期类型”字段格式进行单独过滤清洗处理
     * @param line
     * @return
     */
    public static JSONObject getNotNullJson(String line) {
        JSONObject json = new JSONObject();
        try {
            JSONObject njson = JSONObject.parseObject(line);
            for (String key : njson.keySet()) {
                if (!StringUtils.isEmpty(njson.getString(key)) && !StringUtils.isEmpty(njson.getString(key).trim())) {

                    //对部分日期类型字段格式进行清洗过滤处理
                    if (key.endsWith("Date") || key.endsWith("date") || key.endsWith("Time") || key.endsWith("time") ||
                            key.equals("valFrom") || key.equals("valTo") || key.endsWith("At") || key.endsWith("lastActive")) {

                        if (key.equals("createdAt") || key.equals("updatedAt") || key.equals("checkedAt")) {//"updatedAt":1602630604
                            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            String dateStr = njson.getInteger(key).toString();  //todo 该字段类型存在int或double，统一转Integer (double类型可以直接解析成int类型)
                            String date = format.format(new Date(Long.parseLong(dateStr) * 1000));
                            json.put(key, date);

                        } else {//"estDate":"2002-09-09"
                            String str = njson.getString(key);
                            if (str.length() >= 10) {
                                String s = str.substring(0, 10);
                                String regex = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
//                                Pattern pattern = Pattern.compile(regex);
//                                Matcher m = pattern.matcher(s);
                                if (s.matches(regex)) {
                                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                                    Date date = format.parse(s);
                                    long ts = date.getTime();
                                    long start = format.parse("1800-01-01").getTime();//有这种不合理的数据 "createDate":"0001-01-01"
                                    if (ts >= start) {
                                        String estDate = format.format(new Date(Long.parseLong(String.valueOf(ts))));
                                        json.put(key, estDate);
                                    }
                                }
                            }
                        }
                    } else {
                        json.put(key, njson.getString(key));
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("爬虫日志内容:" + line);
            e.printStackTrace();
        }
        return json;
    }

    /**
     * 解析json字符串成jsonObject，并针对“各种日期类型”字段格式进行单独过滤清洗处理
     * @param line
     * @return
     */
    public static JSONObject getNotNullJsonNew(String line) {
        JSONObject jsonObj = null;
        try {
            jsonObj = JSONObject.parseObject(line);
            // TODO: 2021/1/24 使用新json对象来接收元素比较麻烦，可以考虑使用iterator来处理，直接移除元素 （不能在遍历的时候移除！否则会报modify Exception）
//            Iterator<Map.Entry<String, Object>> iterator = jsonObj.entrySet().iterator();
//            while(iterator.hasNext()){
//                Map.Entry<String, Object> next = iterator.next();
//                String key = next.getKey();
//                Object value = next.getValue();
//                if(key.endsWith("Date")){
//                    jsonObj.put(key, "value");
//                }else{
//                    iterator.remove();
//                }
//            }
            for (String key : jsonObj.keySet()) {
                if (!StringUtils.isEmpty(jsonObj.getString(key)) && !StringUtils.isEmpty(jsonObj.getString(key).trim())) {

                    //对部分日期类型字段格式进行清洗过滤处理
                    if (key.endsWith("Date") || key.endsWith("date") || key.endsWith("Time") || key.endsWith("time") ||
                            key.equals("valFrom") || key.equals("valTo") || key.endsWith("At") || key.endsWith("lastActive")) {

                        if (key.equals("createdAt") || key.equals("updatedAt") || key.equals("checkedAt")) {//"updatedAt":1602630604
                            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            String dateStr = jsonObj.getInteger(key).toString();
                            String date = format.format(new Date(Long.parseLong(dateStr) * 1000));
                            jsonObj.put(key, date);

                        } else {//"estDate":"2002-09-09"
                            String str = jsonObj.getString(key);
                            if (str.length() >= 10) {
                                String s = str.substring(0, 10);
                                String regex = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
                                if (s.matches(regex)) {
                                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                                    Date date = format.parse(s);
                                    Date startDate = format.parse("1800-01-01");//有这种不合理的数据 "createDate":"0001-01-01"
                                    if (DateUtil.compare(date,startDate) > 0) {
                                        jsonObj.put(key, s);
                                    }else {
                                        jsonObj.remove(key);//不要在集合遍历的时候移除元素，会报 java.util.ConcurrentModificationException！
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("爬虫日志内容:" + line);
            e.printStackTrace();
        }
        return jsonObj;
    }

    public static JSONObject getVersion3Json(String line) {
        JSONObject json = new JSONObject();
        try {
            JSONObject newjson = JSONObject.parseObject(line);
            json.put("mobile", newjson.getString("mobile"));
            json.put("channelId", newjson.getString("channelId"));
            json.put("customerId", newjson.getString("customerId"));
            json.put("smUuid", newjson.getString("smUuid"));
            json.put("ctime", newjson.getString("ctime"));
            json.put("batchId", newjson.getString("batchId"));
            json.put("content", newjson.getString("content"));
            json.put("deliverResultReal", newjson.getString("deliverResultReal"));
        } catch (Exception e) {
            //System.out.println("line:"+line);
            e.printStackTrace();

        }
        return json;
    }

    public static String getStr(String line) {
        String s = "";
        JSONObject json = JSONObject.parseObject(line);
        for (String key : json.keySet()) {
            s = s + "\"" + json.getString(key) + "\",";
        }
        s = s.substring(0, s.length() - 1);
        return s;
    }


    public static String[] getJsonKeySet(String line) {
        String[] array = null;
        JSONObject json = new JSONObject();
        try {
            json = JSONObject.parseObject(line);
            array = new String[json.keySet().size()];
            int i = 0;
            for (String key : json.keySet()) {
                array[i] = key;
                i++;
            }
        } catch (Exception e) {
            System.out.println("line:" + line);
            e.printStackTrace();

        }
        return array;
    }

    public static String recoverEncode(String line) {
        JSONObject json = getJson(line);
        try {
            json.put("content", new String(json.getString("content").getBytes("windows-1252"), "gbk"));
            json.put("sign", new String(json.getString("sign").getBytes("windows-1252"), "gbk"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return json.toJSONString();
    }
}
