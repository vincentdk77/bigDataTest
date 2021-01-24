package com.atguigu.kemai.mango;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.kemai.entity.mango.EntItem;
import com.atguigu.kemai.recommend.CategoryItem;
import com.atguigu.kemai.utils.Constant;
import com.atguigu.kemai.utils.ReflectField;
import com.atguigu.kemai.utils.TableName;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class MangoHandle {
    public static Integer Yes = 0;
    public static Integer No = 1;

//    public static String export(JSONObject json) {
//        try {
//            String opScope = json.getString("opScope")
//                    .replace("(", "（")
//                    .replace(")", "）")
//                    .replace("【", "（")
//                    .replace("】", "）")
//                    .replace("许可项目：", "")
//                    .replace("一般项目：", "");
//
//            String[] chars = opScope.split("");
//            ArrayList<Integer> leftIndex = new ArrayList<Integer>();
//            ArrayList<Integer> rightIndex = new ArrayList<Integer>();
//            ArrayList<String> deleteWords = new ArrayList<String>();
//
//            for (int i = 0; i < chars.length; i++) {
//                if (chars[i].equals("（")) {
//                    leftIndex.add(i);
//                } else if (chars[i].equals("）")) {
//                    rightIndex.add(i);
//                }
//            }
//
//            if (leftIndex.size() == rightIndex.size()) {
//                for (int i = 0; i < leftIndex.size(); i++) {
//                    deleteWords.add(opScope.substring(leftIndex.get(i), rightIndex.get(i) + 1));
//                }
//            }
//
//            for (int i = 0; i < deleteWords.size(); i++) {
//                if (StringUtils.isNotEmpty(opScope)) {
//                    opScope = opScope.replace(deleteWords.get(i), "");
//                }
//            }
//
//            if (StringUtils.isNotEmpty(opScope)) {
//                String[] words = opScope.replace(" ", "")
//                        .replace("、", "#")
//                        .replace("；", "#")
//                        .replace(";", "#")
//                        .replace(":", "#")
//                        .replace("：", "#")
//                        .replace("、", "#")
//                        .replace("，", "#")
//                        .replace(",", "#")
//                        .replace("。", "#")
//                        .replace(".", "#")
//                        .replace("^", "#")
//                        .replace("*", "#")
//                        .replace("＊", "#")
//                        .replace("〓", "#")
//                        .split("#");
//                String s = "";
//                for (String word : words) {
//                    if (StringUtils.isNotEmpty(word)) {
//                        s += word + ",";
//                    }
//                }
//                if (s.length() > 1) {
//                    return s.substring(0, s.length() - 1);
//                } else {
//                    return null;
//                }
//            } else {
//                return null;
//            }
//        } catch (Exception e) {
//            System.out.println("--------------------------json:---------------------" + json);
//            return null;
//        }
//    }

    /**
     * ent如果存在多条时，以最新1条数据为准，并合并数据
     */
    public static JSONObject merge(JSONObject json) {
        if (json == null) return null;

        if (json.getJSONArray("ent").size() >= 2) {
            JSONObject ent = json.getJSONArray("ent").getJSONObject(0);

            for (int i = 1; i < json.getJSONArray("ent").size(); i++) {
                JSONObject nextEnt = json.getJSONArray("ent").getJSONObject(i);

                for (String key : nextEnt.keySet()) {
                    if (!StringUtils.isEmpty(nextEnt.getString(key))) {
                        ent.put(key, nextEnt.getString(key));
                    }
                }
            }
            JSONArray newArray = new JSONArray();
            newArray.add(JSONObject.parseObject(JSONObject.toJSONString(ent)));
            json.put("ent", newArray);
        }
        return json;
    }

    /**
     * 自定义行业分类（数据倾斜严重）
     * <p>
     * 1. 如果category不为空，则取分类字典表进行匹配
     * 2. 如果opScope不为空，则取分类字典表进行匹配
     * 3. 否则，即为null
     */
    public static JSONObject customCategory(JSONObject json, ArrayList<CategoryItem> categoryList) {
        if (json == null) return null;

        if (json.getJSONArray("ent") != null) {
            JSONObject ent = json.getJSONArray("ent").getJSONObject(0);
            String oldCategory = ent.getString("category");     //行业分类 原有字段   "category":"专业技术服务业"
            String opScope = ent.getString("opScope");          //经营范围
            boolean isMatched = false;

            if (!StringUtils.isEmpty(oldCategory)) {
                isMatched = match(oldCategory, categoryList, ent);
            }

            // 如果 category 为空或匹配失败，则用 opscope 进行匹配
            if (!isMatched && !StringUtils.isEmpty(opScope)) {
                String[] mark = new String[]{",", "，", ";", "；", "、", "。", ":", "：", "（", "）", "^"};  // 标点符号
                for (String s : mark) {
                    if (opScope.contains(s)) {
                        opScope = opScope.replace(s, "#");
                    }
                }
                String[] words = opScope.split("#");
                for (String word : words) {
                    if (!isMatched && !StringUtils.isEmpty(word)) {
                        isMatched = match(word, categoryList, ent);
                    }
                }
            }
        }
        return json;
    }

    /**
     * 分类匹配和拼接
     */
    public static boolean match(String s, ArrayList<CategoryItem> categoryList, JSONObject ent) {
        boolean isMatched = false;
        //遍历所有分类项
        for (CategoryItem item : categoryList) {
            String categoryName = item.getName();
            //精准匹配，分类字典如果查询到符合的，即中断查询
            if (categoryName.equals(s.trim())) {
                String oneLevelName, twoLevelName, threeLevelName, fourLevelName;
                StringBuilder finalName = new StringBuilder();      //最终拼接的多级分类名
                int length = item.getId().length();     //根据id的长度来判断为几级分类
                //1级分类
                if (length == 1) {
//                    ent.put("industryCategoryId", item.getId());
                    ent.put("industryCategory", categoryName);
                    isMatched = true;
                }
                //2级分类
                else if (length == 2) {
                    oneLevelName = item.getParent().getString("name");
                    twoLevelName = categoryName;
                    finalName.append(oneLevelName).append(",").append(twoLevelName);
                    ent.put("industryCategory", finalName);
                    isMatched = true;
                }
                //3级分类
                else if (length == 3) {
                    oneLevelName = JSONObject.parseObject(item.getParent().getString("parent")).getString("name");
                    twoLevelName = item.getParent().getString("name");
                    threeLevelName = categoryName;
                    finalName.append(oneLevelName).append(",").append(twoLevelName).append(",").append(threeLevelName);
                    ent.put("industryCategory", finalName);
                    isMatched = true;
                }
                //4级分类，如：0123
                else if (length == 4) {
                    oneLevelName = JSONObject.parseObject(JSONObject.parseObject(item.getParent().getString("parent")).getString("parent")).getString("name");
                    twoLevelName = JSONObject.parseObject(item.getParent().getString("parent")).getString("name");
                    threeLevelName = item.getParent().getString("name");
                    fourLevelName = categoryName;
                    finalName.append(oneLevelName).append(",").append(twoLevelName).append(",").append(threeLevelName).append(",").append(fourLevelName);
                    ent.put("industryCategory", finalName);
                    isMatched = true;
                }
                break;
            }
        }
        return isMatched;
    }

    /**
     * 主营产品
     * <p>
     * 若ent的products值不存在，用ent_website的domainKeyword字段替代
     */
    public static JSONObject products(JSONObject json) {
        if (json == null) return null;

        if (json.getJSONArray("ent") != null) {
            JSONObject ent = json.getJSONArray("ent").getJSONObject(0);
            String products = ent.getString("products");

            if (StringUtils.isEmpty(products)) {
                if (json.getJSONArray(TableName.MANGO_ENT_WEBSITE) != null && json.getJSONArray(TableName.MANGO_ENT_WEBSITE).size() > 0) {
                    JSONArray websiteArray = json.getJSONArray("ent_website");

                    for (int i = 0; i < websiteArray.size(); i++) {
                        try {
                            String domainKeyword = websiteArray.getJSONObject(i).getString("domainKeyword");
                            if (!StringUtils.isEmpty(domainKeyword) && !domainKeyword.contains("请填写站点关键词")) {
                                String[] words = domainKeyword.replace("-", "")
                                        .replace("★", "")
                                        .split(",");
                                if (words.length == 1) {
                                    products = "," + words[0];
                                } else if (words.length > 1) {
                                    products = "," + words[0] + "," + words[1];
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                if (products != null) {
                    ent.put("products", products.substring(1));
                }
            }
            JSONArray newArray = new JSONArray();
            newArray.add(JSONObject.parseObject(JSONObject.toJSONString(ent)));
            json.put("ent", newArray);
        }
        return json;
    }

    /**
     * 经营状态：存续、在业、吊销、注销、迁入、迁出、停业、清算
     */
    public static JSONObject corpStatusString(JSONObject json) {
        if (json == null) return null;

        if (json.getJSONArray("ent") != null) {
            JSONObject ent = json.getJSONArray("ent").getJSONObject(0);
            String corpStatusString = ent.getString("corpStatusString");

            if (StringUtils.isNotEmpty(corpStatusString)) {
                if (corpStatusString.startsWith("存续") || corpStatusString.startsWith("在业") || corpStatusString.startsWith("吊销") || corpStatusString.startsWith("注销") ||
                        corpStatusString.startsWith("迁入") || corpStatusString.startsWith("迁出") || corpStatusString.startsWith("停业") || corpStatusString.startsWith("清算")) {
                    ent.put("corpStatusString", corpStatusString.substring(0, 2));
                } else {
                    ent.put("corpStatusString", null);
                }
                JSONArray newArray = new JSONArray();
                newArray.add(JSONObject.parseObject(JSONObject.toJSONString(ent)));
                json.put("ent", newArray);
            }
        }
        return json;
    }

    /**
     * 机构类型
     */
    public static JSONObject agentType(JSONObject json) {
        if (json == null) return null;

        if (json.getJSONArray("ent") != null) {
            JSONObject ent = json.getJSONArray("ent").getJSONObject(0);
            String entTypeCN = ent.getString("entTypeCN");

            if (StringUtils.isNotEmpty(entTypeCN)) {
                //1、港澳台
                if (StringUtils.isEmpty(ent.getString("agentType"))) {
                    String[] gangaotai = Constant.gangaotai.toString().split("#");
                    for (String s : gangaotai) {
                        if (entTypeCN.contains(s)) {
                            ent.put("agentType", "港澳台");
                            break;
                        }
                    }
                }
                //2、外资
                if (StringUtils.isEmpty(ent.getString("agentType"))) {
                    String[] waizi = Constant.waizi.toString().split("#");
                    for (String s : waizi) {
                        if (entTypeCN.contains(s)) {
                            ent.put("agentType", "外资");
                            break;
                        }
                    }
                }
                //3、国营
                if (StringUtils.isEmpty(ent.getString("agentType"))) {
                    String[] guoying = Constant.guoying.toString().split("#");
                    for (String s : guoying) {
                        if (entTypeCN.contains(s)) {
                            ent.put("agentType", "国营");
                            break;
                        }
                    }
                }
                //4、民营
                if (StringUtils.isEmpty(ent.getString("agentType"))) {
                    String[] minying = Constant.minying.toString().split("#");
                    for (String s : minying) {
                        if (entTypeCN.contains(s)) {
                            ent.put("agentType", "民营");
                            break;
                        }
                    }
                }
                //5、农资
                if (StringUtils.isEmpty(ent.getString("agentType"))) {
                    String[] nongzi = Constant.nongzi.toString().split("#");
                    for (String s : nongzi) {
                        if (entTypeCN.contains(s)) {
                            ent.put("agentType", "农资");
                            break;
                        }
                    }
                }
                //6、个体户
                if (StringUtils.isEmpty(ent.getString("agentType"))) {
                    String[] geti = Constant.geti.toString().split("#");
                    for (String s : geti) {
                        if (entTypeCN.contains(s)) {
                            ent.put("agentType", "个体户");
                            break;
                        }
                    }
                }
                //7、社会团体
                if (StringUtils.isEmpty(ent.getString("agentType"))) {
                    String[] shehui = Constant.shehui.toString().split("#");
                    for (String s : shehui) {
                        if (entTypeCN.contains(s)) {
                            ent.put("agentType", "社会团体");
                            break;
                        }
                    }
                }
                // 港、澳、台
                if (StringUtils.isEmpty(ent.getString("agentType")) && (entTypeCN.contains("港") || entTypeCN.contains("澳") || entTypeCN.contains("台"))) {
                    ent.put("agentType", "港澳台");
                }
                // 外国、外资
                if (StringUtils.isEmpty(ent.getString("agentType")) && (entTypeCN.contains("外国") || entTypeCN.contains("外资"))) {
                    ent.put("agentType", "外资");
                }
                // 国有、国营、集体、公有制、全民所有制
                if (StringUtils.isEmpty(ent.getString("agentType")) && (entTypeCN.contains("国有") || entTypeCN.contains("国营") || entTypeCN.contains("集体") || entTypeCN.contains("公有制") || entTypeCN.contains("全民所有制"))) {
                    ent.put("agentType", "国营");
                }
                // 私人、民营、自然人
                if (StringUtils.isEmpty(ent.getString("agentType")) && (entTypeCN.contains("私人") || entTypeCN.contains("民营") || entTypeCN.contains("自然人"))) {
                    ent.put("agentType", "民营");
                }
                // 农业、农资、农民
                if (StringUtils.isEmpty(ent.getString("agentType")) && (entTypeCN.contains("农业") || entTypeCN.contains("农资") || entTypeCN.contains("农民"))) {
                    ent.put("agentType", "农资");
                }
                // 个人、个体
                if (StringUtils.isEmpty(ent.getString("agentType")) && (entTypeCN.contains("个人") || entTypeCN.contains("个体"))) {
                    ent.put("agentType", "个体户");
                }
                // 协会、团、组织、机构
                if (StringUtils.isEmpty(ent.getString("agentType")) && (entTypeCN.contains("协会") || entTypeCN.contains("团") || entTypeCN.contains("组织") || entTypeCN.contains("机构"))) {
                    ent.put("agentType", "社会团体");
                }
                //8、其他类型
                if (StringUtils.isEmpty(ent.getString("agentType"))) {
                    ent.put("agentType", "其他类型");
                }
            }

            JSONArray newArray = new JSONArray();
            newArray.add(JSONObject.parseObject(JSONObject.toJSONString(ent)));
            json.put("ent", newArray);
        }
        return json;
    }

    /**
     * 检查部分字段是否转Integer或Double
     */
    public static JSONObject check(JSONObject json) {
        if (json == null) return null;

        if (json.getJSONArray("ent") != null) {
            JSONObject ent = json.getJSONArray("ent").getJSONObject(0);
            if (!StringUtils.isEmpty(ent.getString("regCaption"))) {
                try {
                    BigDecimal bigDecimal = new BigDecimal(ent.getString("regCaption"));
                    BigDecimal regCaption = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP);
                    ent.put("regCaption", regCaption);
                } catch (Exception e) {
                    ent.put("regCaption", null);
                }
            }
            if (!StringUtils.isEmpty(ent.getString("employeesMin"))) {
                try {
                    Integer employeesMin = Integer.valueOf(ent.getString("employeesMin"));
                    ent.put("employeesMin", employeesMin);
                } catch (Exception e) {
                    ent.put("employeesMin", null);
                }
            }
            if (!StringUtils.isEmpty(ent.getString("employeesMax"))) {
                try {
                    Integer employeesMax = Integer.valueOf(ent.getString("employeesMax"));
                    ent.put("employeesMax", employeesMax);
                } catch (Exception e) {
                    ent.put("employeesMax", null);
                }
            }
        }
        return json;
    }

    public static JSONObject setMangoId(JSONObject json) {
        if (json == null) return null;

        if (json.getJSONArray("ent") != null) {
            JSONObject ent = json.getJSONArray("ent").getJSONObject(0);

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Date date = new Date();

            EntItem entItem = new EntItem();
            ReflectField.setField(ent, entItem);
            setDefault(entItem);

            // 统计字段
            for (String key : json.keySet()) {

                /**
                 * A级纳税人：
                 * isTaxARate：是否A级纳税人，默认为0
                 */
                if (TableName.MANGO_ENT_A_TAXPAYER.equals(key)) {
                    entItem.setIsTaxARate(1);
                }

                /**
                 * 经营异常：
                 * isAbnormally：当前是否被列入经营异常，默认为0
                 * noAbnomalyOpt：有无经营异常，默认为1
                 * abnormalCount：经营异常数量
                 */
                if (TableName.MANGO_ENT_ABNORMAL_OPT.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);
                    int isAbnormal = 0;
                    for (int i = 0; i < currentArray.size(); i++) {
                        JSONObject currentJson = currentArray.getJSONObject(i);
                        //如果abntime不为空，而remDate为空，则该异常记录存在且未被移除
                        if (StringUtils.isNotEmpty(currentJson.getString("abntime")) && StringUtils.isEmpty(currentJson.getString("remDate"))) {
                            isAbnormal = 1;
                        }
                    }
                    entItem.setIsAbnormally(isAbnormal);
                    if (isAbnormal == 1) {
                        entItem.setNoAbnomalyOpt(0);
                    }
                    entItem.setAbnormalCount(currentArray.size());
                }

                /**
                 * 年报：
                 * noAnnualReport：有无企业年报，默认为1
                 */
                if (TableName.MANGO_ENT_ANNUAL_REPORT.equals(key)) {
                    entItem.setNoAnnualReport(0);
                }

                /**
                 * 应用：
                 * noApp： 有无应用，默认为1
                 * appCount：应用数量
                 * appScoreAvg：应用平均分
                 * appDownloadCount：应用总下载量
                 */
                if (TableName.MANGO_ENT_APPS.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);
                    double appScore = 0;    //当前应用分数
                    double sumScore = 0;  //所有应用总分
                    int downloadCount = 0;  //当前应用下载数
                    int sumDownloadCount = 0;  //所有应用下载总数
                    for (int i = 0; i < currentArray.size(); i++) {
                        JSONObject currentJson = currentArray.getJSONObject(i);
                        if (StringUtils.isNotEmpty(currentJson.getString("appFraction"))) {
                            try {
                                appScore = Double.parseDouble(currentJson.getString("appFraction"));
                            } catch (NumberFormatException e) {
                                appScore = 0.0;
                            }
                            sumScore += appScore;
                        }
                        if (StringUtils.isNotEmpty(currentArray.getJSONObject(i).getString("appDownloadCount"))) {
                            try {
                                downloadCount = Integer.parseInt(currentJson.getString("appDownloadCount"));
                            } catch (NumberFormatException e) {
                                downloadCount = 0;
                            }
                            sumDownloadCount += downloadCount;
                        }
                    }
                    entItem.setNoApp(0);
                    entItem.setAppCount(currentArray.size());
                    entItem.setAppScoreAvg(sumScore / currentArray.size());
                    entItem.setAppDownloadCount(sumDownloadCount);
                }

                /**
                 * 招投标：
                 * noBidding：有无招投标，默认为1
                 */
                if (TableName.MANGO_ENT_BIDS.equals(key)) {
                    entItem.setNoBidding(0);
                }

                /**
                 * 品牌：
                 * noBrand：有无品牌信息，默认为1
                 * brandCount：品牌数量
                 */
                if (TableName.MANGO_ENT_BRAND.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);
                    entItem.setNoBrand(0);
                    entItem.setBrandCount(currentArray.size());
                }

                /**
                 * 资质证书：
                 * certType：证书类型
                 * noCert：有无证书，默认为1
                 * certCount：证书数量：存在记录数
                 * certTypeLTM：最近3个月内截止证书类别
                 * certTypeLSM：最近6个月内截止证书类别
                 * certThisYearCount：本年度获证数量：pubDate不为空且小于一年
                 */
                if (TableName.MANGO_ENT_CERT.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);

                    HashSet<String> certTypeSet = new HashSet<String>();
                    HashSet<String> certTypeLTMSet = new HashSet<String>();
                    HashSet<String> certTypeLSMSet = new HashSet<String>();

                    StringBuilder certTypeStr = new StringBuilder();
                    StringBuilder certTypeLTMStr = new StringBuilder();
                    StringBuilder certTypeLSMStr = new StringBuilder();

                    Integer certThisYearCount = 0;//本年度获证数量
                    long lastTime = 0;

                    for (int i = 0; i < currentArray.size(); i++) {
                        JSONObject currentJson = currentArray.getJSONObject(i);
                        String pubDate = currentJson.getString("pubDate");

                        if (StringUtils.isNotEmpty(currentJson.getString("certType"))) {
                            certTypeSet.add(currentJson.getString("certType"));
                        }

                        if (StringUtils.isNotEmpty(pubDate)) {
                            long pubTime = 0;
                            try {
                                pubTime = format.parse(pubDate).getTime();
                            } catch (ParseException e) {
                            }
                            // 最近3个月内截止证书类别
                            if ((date.getTime() - pubTime) < 90 * 24 * 3600 * 1000 && pubTime > lastTime) {
                                lastTime = pubTime;
                                if (StringUtils.isNotEmpty(currentJson.getString("certCategory"))) {
                                    certTypeLTMSet.add(currentJson.getString("certCategory"));
                                }
                            }
                            // 最近6个月内截止证书类别
                            if ((date.getTime() - pubTime) < 180 * 24 * 3600 * 1000 && pubTime > lastTime) {
                                lastTime = pubTime;
                                if (StringUtils.isNotEmpty(currentJson.getString("certCategory"))) {
                                    certTypeLSMSet.add(currentJson.getString("certCategory"));
                                }
                            }
                            // 本年度获证数量
                            if ((date.getTime() - pubTime) < 365 * 24 * 3600 * 1000) {
                                certThisYearCount++;
                            }
                        }
                    }
                    for (String s : certTypeSet) {
                        certTypeStr.append(",").append(s);
                    }
                    for (String s : certTypeLTMSet) {
                        certTypeLTMStr.append(",").append(s);
                    }
                    for (String s : certTypeLSMSet) {
                        certTypeLSMStr.append(",").append(s);
                    }
                    if (StringUtils.isNotEmpty(certTypeStr)) {
                        entItem.setCertType(certTypeStr.substring(1));
                    }
                    if (StringUtils.isNotEmpty(certTypeLTMStr)) {
                        entItem.setCertTypeLTM(certTypeLTMStr.substring(1));
                    }
                    if (StringUtils.isNotEmpty(certTypeLSMStr)) {
                        entItem.setCertTypeLSM(certTypeLSMStr.substring(1));
                    }
                    entItem.setNoCert(0);
                    entItem.setCertCount(currentArray.size());
                    entItem.setCertThisYearCount(certThisYearCount);
                }

                /**
                 * 版权：
                 * noCr：有无作品著作权
                 * crCount：作品著作权数量
                 */
                if (TableName.MANGO_ENT_COPYRIGHTS.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);
                    entItem.setNoCr(0);
                    entItem.setCrCount(currentArray.size());
                }

                /**
                 * 联系方式：
                 * noMobile：有无手机：type=1，符合条件即为0，否则不赋值
                 * noTel：有无固话：type=2，符合条件即为0，否则不赋值
                 * noEmail：有无邮箱：type=3，符合条件即为0，否则不赋值
                 * contactCount：联系人数量：type!=6，符合条件记录数
                 */
                if (TableName.MANGO_ENT_CONTACTS.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);
                    int contactCount = currentArray.size();
                    for (int i = 0; i < currentArray.size(); i++) {
                        JSONObject currentJson = currentArray.getJSONObject(i);
                        if ("1".equals(currentJson.getString("type"))) {
                            entItem.setNoMobile(0);
                        }
                        if ("2".equals(currentJson.getString("type"))) {
                            entItem.setNoTel(0);
                        }
                        if ("3".equals(currentJson.getString("type"))) {
                            entItem.setNoEmail(0);
                        }
                        if ("6".equals(currentArray.getJSONObject(i).getString("type"))) {
                            contactCount--;
                        }
                    }
                    entItem.setContactCount(contactCount);
                }

                /**
                 * 法院公告：
                 * noCourtNotice：
                 */
                if (TableName.MANGO_ENT_COURT_NOTICE.equals(key)) {
                    entItem.setNoCourtNotice(0);
                }

                /**
                 * 裁判文书：
                 * noJudicialPaper：
                 */
                if (TableName.MANGO_ENT_COURT_PAPER.equals(key)) {
                    entItem.setNoJudicialPaper(0);
                }

                /**
                 * 电商店铺：
                 * ecShopCount：电商店铺数量
                 */
                if (TableName.MANGO_ENT_ECOMMERCE.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);
                    entItem.setEcShopCount(currentArray.size());
                }

                /**
                 * 股权出资：
                 * noEquityPledge：有无股权出资：存在即为0，否则不赋值
                 */
                if (TableName.MANGO_ENT_EQUITY_PLEDGED.equals(key)) {
                    entItem.setNoEquityPledge(0);
                }

                /**
                 * 融资事件：
                 * stag：融资轮次
                 * financingCount：融资次数
                 */
                if (TableName.MANGO_ENT_FUNDING_EVENT.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);
                    long lastTime = 0;
                    String stag = null;
                    for (int i = 0; i < currentArray.size(); i++) {
                        JSONObject currentJson = currentArray.getJSONObject(i);
                        String pubDate = currentJson.getString("pubDate");
                        if (StringUtils.isNotEmpty(pubDate)) {
                            long current = 0;
                            try {
                                current = format.parse(pubDate).getTime();
                            } catch (ParseException e) {
                            }
                            if (current > lastTime) {
                                lastTime = current;
                                stag = currentJson.getString("stag");
                            }
                        }
                        entItem.setStag(stag);
                    }
                    entItem.setFinancingCount(currentArray.size());
                }

                /**
                 * 商品：
                 * noGoods：有无商品
                 * goodsCount：商品数量
                 */
                if (TableName.MANGO_ENT_GOODS.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);
                    entItem.setNoGoods(0);
                    entItem.setGoodsCount(currentArray.size());
                }

                /**
                 * 成长型企业：
                 * isGazelle：是否瞪羚企业：category=1，符合即为1
                 * isUnicorn：是否独角兽企业：category=2，符合即为1
                 * isHighTech：是否为高新企业：category=3，符合即为1
                 */
                if (TableName.MANGO_ENT_GROWINGUP.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);
                    for (int i = 0; i < currentArray.size(); i++) {
                        JSONObject currentJson = currentArray.getJSONObject(i);
                        String category = currentJson.getString("category");
                        if (category.equals("1")) {
                            entItem.setIsGazelle(1);
                            break;
                        } else if (category.equals("2")) {
                            entItem.setIsUnicorn(1);
                            break;
                        } else if (category.equals("3")) {
                            entItem.setIsHighTech(1);
                            break;
                        }
                    }
                }

                /**
                 * 对外投资：
                 * noInvest：有无对外投资：brunch不为空且isBrunch=0，符合即为0
                 * branchCount：分支机构数：brunch不为空且isBrunch=1，符合条件的记录数
                 */
                if (TableName.MANGO_ENT_INVESTCOMPANY.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);
                    int branchCount = 0;

                    for (int i = 0; i < currentArray.size(); i++) {
                        JSONObject currentJson = currentArray.getJSONObject(i);
                        if (StringUtils.isNotEmpty(currentJson.getString("brunch"))) {
                            if (currentJson.getString("isBrunch").equals("1")) {
                                branchCount++;
                            }
                            if (currentJson.getString("isBrunch").equals("0")) {
                                entItem.setNoInvest(0);
                            }
                        }
                        entItem.setBranchCount(branchCount);
                    }
                }

                /**
                 * 行政许可：
                 * noLicence：有无行政许可
                 * licCount：行政许可数量
                 */
                if (TableName.MANGO_ENT_LICENCE.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);
                    entItem.setNoLicence(0);
                    entItem.setLicCount(currentArray.size());
                }

                /**
                 * 上市公司：
                 * isStock：是否上市企业：存在即为1，否则不赋值
                 */
                if (TableName.MANGO_ENT_LISTED.equals(key)) {
                    entItem.setIsStock(No);
                }

                /**
                 * 新媒体：
                 * noWeMedia：有无自媒体：存在即为0，否则不赋值
                 * noWechat：有无微信公众号：type=1，存在即为0，否则不赋值
                 * noWeibo：有无微博：type=2，存在即为0，否则不赋值
                 * wechatCount：微信公众号数：type=1，存在记录数
                 * weiboOrgCount：机构微博数：type=2，存在记录数
                 */
                if (TableName.MANGO_ENT_NEW_MEDIA.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);
                    int wechatCount = 0;
                    int weiboOrgCount = 0;

                    for (int i = 0; i < currentArray.size(); i++) {
                        JSONObject currentJson = currentArray.getJSONObject(i);
                        if ("1".equals(currentJson.getString("type"))) {
                            wechatCount++;
                            entItem.setNoWechat(Yes);
                        }
                        if ("2".equals(currentJson.getString("type"))) {
                            weiboOrgCount++;
                            entItem.setNoWeibo(Yes);
                        }
                    }
                    entItem.setNoWeMedia(0);
                    entItem.setWeiboOrgCount(weiboOrgCount);
                    entItem.setWechatCount(wechatCount);
                }

                /**
                 * 新闻：
                 * noNews：有无新闻：存在即为0，否则不赋值
                 * lMNewsCount：近1月新闻数量：pubDate小于1个月，符合条件的记录数
                 */
                if (TableName.MANGO_ENT_NEWS.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);
                    int lMNewsCount = 0;//近1月新闻数量

                    for (int i = 0; i < currentArray.size(); i++) {
                        JSONObject currentJson = currentArray.getJSONObject(i);
                        String pubDate = currentJson.getString("pubDate");
                        if (StringUtils.isNotEmpty(pubDate)) {
                            try {
                                if (date.getTime() - format.parse(pubDate).getTime() < 30 * 24 * 3600 * 1000) {
                                    lMNewsCount++;
                                }
                            } catch (ParseException e) {
                            }
                        }
                    }
                    entItem.setNoNews(Yes);
                    entItem.setlMNewsCount(lMNewsCount);
                }

                /**
                 * 专利：
                 * noPatent：有无专利
                 * patentCount：专利数量
                 * lYPatentCount：最近一年申请专利数量：appDate不为空且小于一年
                 * lYPatentPublicCount：最近一年公开专利数量：pubDate不为空且小于一年
                 */
                if (TableName.MANGO_ENT_PATENT.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);

                    Integer lYPatentCount = 0;
                    Integer lYPatentPublicCount = 0;

                    for (int i = 0; i < currentArray.size(); i++) {
                        JSONObject currentJson = currentArray.getJSONObject(i);
                        String appDate = currentJson.getString("appDate");
                        String pubDate = currentJson.getString("pubDate");
                        if (!StringUtils.isEmpty(appDate)) {
                            try {
                                if (date.getTime() - format.parse(appDate).getTime() < 365 * 24 * 3600000l) {
                                    lYPatentCount++;
                                }
                            } catch (ParseException e) {
                                currentJson.put("appDate", null);
                            }
                        } else {
                            currentJson.put("appDate", null);
                        }
                        if (StringUtils.isNotEmpty(pubDate)) {
                            try {
                                if (date.getTime() - format.parse(pubDate).getTime() < 365 * 24 * 3600000l) {
                                    lYPatentPublicCount++;
                                }
                            } catch (ParseException e) {
                                currentJson.put("pubDate", null);
                            }
                        } else {
                            currentJson.put("pubDate", null);
                        }
                    }
                    entItem.setNoPatent(0);
                    entItem.setPatentCount(currentArray.size());
                    entItem.setlYPatentCount(lYPatentCount);
                    entItem.setlYPatentPublicCount(lYPatentPublicCount);
                }

                /**
                 * 行政处罚：
                 * noPunishment：有无行政处罚
                 * punishmentCount：行政处罚数量
                 * noIllegal：有无严重违法
                 */
                if (TableName.MANGO_ENT_PUNISHMENT.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);

                    for (int i = 0; i < currentArray.size(); i++) {
                        JSONObject currentJson = currentArray.getJSONObject(i);
                        String illegaContent = currentJson.getString("illegaContent");
                        if (StringUtils.isNotEmpty(illegaContent)) {
                            entItem.setNoIllegal(0);
                        }
                    }
                    entItem.setNoPunishment(0);
                    entItem.setPunishmentCount(currentArray.size());
                }

                /**
                 * 招聘：
                 * salaryAvg：岗位平均薪酬
                 * postUpdateTimeAvg：岗位平均更新频率
                 * postCount：当前招聘岗位数
                 * recruitCount：当前招聘人数
                 * lTMPostCount：近三个月招聘岗位数
                 * lTMJobCount：近三个月招聘总人数
                 * recruitTitleLM：最近1个月招聘岗位名称
                 */
                if (TableName.MANGO_ENT_RECRUIT.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);

                    double salaryAvg = 0.0;     //平均薪酬
                    int salaryNum = 0;          //参与计算的数量
                    long lastPubTime = 0;       //最新一次更新时间
                    int days = 0;               //天数
                    int num = 0;                //招聘人数，如字段不存在或无法解析，默认该岗位招1人
                    int lTMPostCount = 0;       //近三个月招聘岗位数
                    int lTMJobCount = 0;        //近三个月招聘总人数
                    StringBuilder recruitTitleLM = new StringBuilder();

                    for (int i = 0; i < currentArray.size(); i++) {
                        JSONObject currentJson = currentArray.getJSONObject(i);

                        Double salaryFrom;
                        if (StringUtils.isNotEmpty(currentJson.getString("salaryFrom"))) {
                            try {
                                salaryFrom = Double.parseDouble(currentJson.getString("salaryFrom"));
                            } catch (NumberFormatException e) {
                                salaryFrom = null;
                            }
                        } else {
                            salaryFrom = null;
                        }

                        Double salaryTo;
                        if (StringUtils.isNotEmpty(currentJson.getString("salaryTo"))) {
                            try {
                                salaryTo = Double.parseDouble(currentJson.getString("salaryTo"));
                            } catch (NumberFormatException e) {
                                salaryTo = null;
                            }
                        } else {
                            salaryTo = null;
                        }

                        //岗位平均薪酬
                        if (salaryFrom != null && salaryTo == null) {
                            salaryAvg += salaryFrom;
                            salaryNum++;
                        } else if (salaryFrom == null && salaryTo != null) {
                            salaryAvg += salaryTo;
                            salaryNum++;
                        } else if (salaryFrom != null && salaryTo != null) {
                            salaryAvg += (salaryFrom + salaryTo) / 2;
                            salaryNum++;
                        }

                        Long rdate;
                        if (StringUtils.isNotEmpty(currentJson.getString("rdate"))) {
                            try {
                                rdate = format.parse(currentJson.getString("rdate")).getTime();  //发布日期
                            } catch (ParseException e) {
                                rdate = null;
                            }
                        } else {
                            rdate = null;
                        }

                        //岗位平均更新频率
                        if (rdate != null) {
                            if (rdate > lastPubTime) {
                                lastPubTime = rdate;
                            }
                        }

                        //当前招聘人数
                        if (StringUtils.isNotEmpty(currentJson.getString("num"))) {
                            try {
                                num += Integer.parseInt(currentJson.getString("num"));
                            } catch (NumberFormatException e) {
                                num += 1;
                            }
                        } else {
                            num += 1;
                        }

                        //近三个月招聘岗位数
                        if (rdate != null) {
                            if ((date.getTime() - rdate) < 90 * 24 * 3600 * 1000) {
                                lTMPostCount++;
                            }
                        }

                        //近三个月招聘总人数
                        if (rdate != null) {
                            if ((date.getTime() - rdate) < 90 * 24 * 3600 * 1000) {
                                if (StringUtils.isNotEmpty(currentJson.getString("num"))) {
                                    try {
                                        lTMJobCount += Integer.parseInt(currentJson.getString("num"));
                                    } catch (NumberFormatException e) {
                                        lTMJobCount += 1;
                                    }
                                } else {
                                    lTMJobCount += 1;
                                }
                            }
                        }

                        //最近1个月招聘岗位名称
                        String title = currentJson.getString("title");
                        if (rdate != null) {
                            if (StringUtils.isNotEmpty(title) && (date.getTime() - rdate) < 30 * 24 * 3600 * 1000) {
                                recruitTitleLM.append(",").append(title);
                            }
                        }
                    }

                    //统计结果赋值
                    if (salaryNum != 0) {
                        entItem.setSalaryAvg(salaryAvg / salaryNum);
                    }
                    days = (int) ((date.getTime() - lastPubTime) / 24 * 3600 * 1000);
                    entItem.setPostUpdateTimeAvg(days + "");
                    entItem.setPostCount(currentArray.size());
                    entItem.setRecruitCount(num);
                    entItem.setlTMPostCount(lTMPostCount);
                    entItem.setlTMJobCount(lTMJobCount);
                    if (StringUtils.isNotEmpty(recruitTitleLM.toString())) {
                        entItem.setRecruitTitleLM(recruitTitleLM.substring(1));
                    }
                }

                /**
                 * 软著：
                 * noSr：有无软著：存在即为0，否则不赋值
                 * srCount：软著数量
                 */
                if (TableName.MANGO_ENT_SOFTWARE.equals(key)) {
                    entItem.setNoSr(0);
                    entItem.setSrCount(json.getJSONArray(key).size());
                }

                /**
                 * 500强：
                 * isTop500：是否500强企业：存在即为1，否则不赋值
                 */
                if (TableName.MANGO_ENT_TOP500.equals(key)) {
                    entItem.setIsTop500(No);
                }

                /**
                 * 商标：
                 * noTrademark：有无商标：存在即为0，否则不赋值
                 * tmValidCount：有效状态商标数量
                 * tmInvalidCount：无效状态商标数量
                 */
                if (TableName.MANGO_ENT_TRADEMARK.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);
                    int tmValidCount = 0;
                    int tmInvalidCount = 0;

                    for (int i = 0; i < currentArray.size(); i++) {
                        JSONObject currentJson = currentArray.getJSONObject(i);
                        String propertyEndDate = currentJson.getString("propertyEndDate");
                        if (StringUtils.isNotEmpty(propertyEndDate)) {
                            try {
                                if ((date.getTime() - format.parse(propertyEndDate).getTime()) <= 0) {
                                    tmValidCount++;
                                } else {
                                    tmInvalidCount++;
                                }
                            } catch (ParseException e) {
                            }
                        }
                    }
                    entItem.setNoTrademark(Yes);
                    entItem.setTmValidCount(tmValidCount);
                    entItem.setTmValidCount(tmInvalidCount);
                }

                /**
                 * 网站：
                 * noICP：有无ICP备案：icpList不为空
                 * iCPCount：ICP备案数量：icpList数组大小
                 * noDomain：有无官网：domain不为空，符合条件即为0，否则不赋值
                 * noOnlineService：有无在线客服：mCc=1
                 * noHttps：有无Https模块：mHttps=1
                 * domainAgeCount：域名年龄：ipList - regDate（登记日期），当前日期减去登记日期，再取ceil函数
                 */
                if (TableName.MANGO_ENT_WEBSITE.equals(key)) {
                    JSONArray currentArray = json.getJSONArray(key);

                    int noICP = 1;
                    int iCPCount = 0;   // ICP备案数量
                    int maxAge = 0;     // 域名年龄

                    for (int i = 0; i < currentArray.size(); i++) {
                        JSONObject currentJson = currentArray.getJSONObject(i);
                        entItem.setDomain(currentJson.getString("domain"));

                        //有无官网
                        if (StringUtils.isNotEmpty(currentJson.getString("domain"))) {
                            entItem.setNoDomain(0);
                        }
                        //有无客服
                        if (StringUtils.isNotEmpty(currentJson.getString("mCc")) && "1".equals(currentJson.getString("mCc"))) {
                            entItem.setNoOnlineService(0);
                        }
                        //有无https
                        if (!StringUtils.isNotEmpty(currentJson.getString("mHttps")) && "1".equals(currentJson.getString("mHttps"))) {
                            entItem.setNoHttps(0);
                        }

                        try {
                            if (currentJson.getJSONArray("icpList") != null && currentJson.getJSONArray("icpList").size() > 0) {
                                noICP = 0;
                                iCPCount += currentJson.getJSONArray("icpList").size();

                                JSONArray icpList = currentJson.getJSONArray("icpList");
                                if (icpList != null) {
                                    for (int j = 0; j < icpList.size(); j++) {
                                        JSONObject icpJson = icpList.getJSONObject(j);
                                        String regDate = icpJson.getString("regDate");
                                        if (StringUtils.isNotEmpty(regDate)) {
                                            Double age = null;
                                            try {
                                                age = Math.ceil((date.getTime() - format.parse(regDate).getTime()) / 365 * 24 * 3600000f);
                                            } catch (ParseException e) {
                                                age = 0.0;
                                            }
                                            if (age.intValue() > maxAge) {
                                                maxAge = age.intValue();
                                            }
                                        }
                                    }
                                }
                            }
                        } catch (Exception e) {
                            System.out.println("icpList并非JSONArray类型，格式有误！");
                        }

                        entItem.setNoICP(noICP);
                        entItem.setiCPCount(iCPCount);

                        if (maxAge > 0) {
                            entItem.setDomainAgeCount(maxAge);
                        }
                    }
                }
            }

            String sysTags = "";

            // 企业状态
            if (!StringUtils.isEmpty(entItem.getCorpStatusString())) {
                sysTags = sysTags + entItem.getCorpStatusString().trim() + ",";
            }
            // 经营异常
            if (entItem.getIsAbnormally() != null && entItem.getIsAbnormally() == 1) {
                sysTags = sysTags + "经营异常" + ",";
            }
            // 500强企业
            if (entItem.getIsTop500() != null && entItem.getIsTop500() == 1) {
                sysTags = sysTags + "500强企业" + ",";
            }
            // 瞪羚企业
            if (entItem.getIsGazelle() != null && entItem.getIsGazelle() == 1) {
                sysTags = sysTags + "瞪羚企业" + ",";
            }
            // 独角兽企业
            if (entItem.getIsUnicorn() != null && entItem.getIsUnicorn() == 1) {
                sysTags = sysTags + "独角兽企业" + ",";
            }
            // 高新企业
            if (entItem.getIsHighTech() != null && entItem.getIsHighTech() == 1) {
                sysTags = sysTags + "高新企业" + ",";
            }
            // 上市企业
            if (entItem.getIsStock() != null && entItem.getIsStock() == 1) {
                sysTags = sysTags + "上市企业" + ",";
            }
            // 融资轮次
            if (!StringUtils.isEmpty(entItem.getStag())) {
                sysTags = sysTags + entItem.getStag().trim() + ",";
            }
            // A级纳税人
            if (entItem.getIsTaxARate() != null && entItem.getIsTaxARate() == 1) {
                sysTags = sysTags + "A级纳税人" + ",";
            }
            // 机构类型
            if (!StringUtils.isEmpty(entItem.getAgentType())) {
                sysTags = sysTags + entItem.getAgentType().trim() + ",";
            }
            // 注册资本
            if (!StringUtils.isEmpty(entItem.getRegCaption()) && StringUtils.isNotEmpty(entItem.getRegCapCurCN())) {
                if (entItem.getRegCapCurCN().equals("人民币")) {
                    sysTags = sysTags + "注册资本" + entItem.getRegCaption() + "万元,";
                } else {
                    sysTags = sysTags + "注册资本" + entItem.getRegCaption() + "万" + entItem.getRegCapCurCN() + ",";
                }
            }
            // 公司人数
            if (!StringUtils.isEmpty(entItem.getEmployeesMin()) && !StringUtils.isEmpty(entItem.getEmployeesMax()) && !entItem.getEmployeesMax().equals("0")) {
                sysTags = sysTags + "公司人数" + entItem.getEmployeesMin() + "-" + entItem.getEmployeesMax() + "人" + ",";
            }
            // 资质证书
            if (!StringUtils.isEmpty(entItem.getCertType())) {
                sysTags = sysTags + entItem.getCertType().trim() + ",";
            }
            // 自定义行业分类
            if (!StringUtils.isEmpty(entItem.getIndustryCategory())) {
                sysTags = sysTags + entItem.getIndustryCategory().trim() + ",";
            }
            // 主营产品
            if (!StringUtils.isEmpty(entItem.getProducts())) {
                sysTags = sysTags + entItem.getProducts().trim() + ",";
            }

            if (!StringUtils.isEmpty(sysTags)) {
                sysTags = sysTags.substring(0, sysTags.length() - 1);
            }
            entItem.setSystemTags(sysTags);

            JSONArray newEntArray = new JSONArray();
            JSONObject newEnt = JSONObject.parseObject(JSON.toJSONString(entItem));
            newEntArray.add(newEnt);
            json.put("ent", newEntArray);
        }
        return json;
    }

    public static JSONObject delete(JSONObject json) {

        JSONObject newJson = new JSONObject();
        JSONArray array = new JSONArray();
        JSONObject newEnt = new JSONObject();
        if (json != null && json.getJSONArray("ent") != null) {
            JSONObject ent = json.getJSONArray("ent").getJSONObject(0);
            newEnt.put("entId", ent.getString("entId"));
            newEnt.put("_id", ent.getString("_id"));
            newEnt.put("branchCount", ent.getString("branchCount"));
            newEnt.put("licCount", ent.getString("licCount"));
            newEnt.put("insuredCount", ent.getString("insuredCount"));
            newEnt.put("contactCount", ent.getString("contactCount"));
            newEnt.put("postCount", ent.getString("postCount"));
            newEnt.put("recruitCount", ent.getString("recruitCount"));
            newEnt.put("iCPCount", ent.getString("iCPCount"));
            newEnt.put("lTMPostCount", ent.getString("lTMPostCount"));
            newEnt.put("lTMJobCount", ent.getString("lTMJobCount"));
            newEnt.put("certCount", ent.getString("certCount"));
            newEnt.put("weiboOrgCount", ent.getString("weiboOrgCount"));
            newEnt.put("lMNewsCount", ent.getString("lMNewsCount"));
            newEnt.put("prmtKeyCount", ent.getString("prmtKeyCount"));
            newEnt.put("prmtCompCount", ent.getString("prmtCompCount"));
            newEnt.put("itemCount", ent.getString("itemCount"));
            newEnt.put("goodsCount", ent.getString("goodsCount"));

            newEnt.put("appCount", ent.getString("appCount"));
            newEnt.put("brandCount", ent.getString("brandCount"));
            newEnt.put("abnormalCount", ent.getString("abnormalCount"));
            newEnt.put("punishmentCount", ent.getString("punishmentCount"));
            newEnt.put("tmValidCount", ent.getString("tmValidCount"));
            newEnt.put("tmInvalidCount", ent.getString("tmInvalidCount"));
            newEnt.put("patentCount", ent.getString("patentCount"));
            newEnt.put("lYPatentCount", ent.getString("lYPatentCount"));
            newEnt.put("lYPatentPublicCount", ent.getString("lYPatentPublicCount"));
            newEnt.put("srCount", ent.getString("srCount"));
            newEnt.put("crCount", ent.getString("crCount"));
            newEnt.put("wechatCount", ent.getString("wechatCount"));
            newEnt.put("financingCount", ent.getString("financingCount"));
            newEnt.put("appDownloadCount", ent.getString("appDownloadCount"));
            newEnt.put("certThisYearCount", ent.getString("certThisYearCount"));

            newEnt.put("prmtLinksCount", ent.getString("prmtLinksCount"));
            newEnt.put("ecShopCount", ent.getString("ecShopCount"));
            newEnt.put("domainAgeCount", ent.getString("domainAgeCount"));
            newEnt.put("appScoreAvg", ent.getString("appScoreAvg"));
            newEnt.put("salaryAvg", ent.getString("salaryAvg"));
            newEnt.put("postUpdateTimeAvg", ent.getString("postUpdateTimeAvg"));
            newEnt.put("errorRateAvg", ent.getString("errorRateAvg"));
            newEnt.put("respTimeAvg", ent.getString("respTimeAvg"));
            newEnt.put("isHighTech", ent.getString("isHighTech"));
            newEnt.put("isTop500", ent.getString("isTop500"));
            newEnt.put("isUnicorn", ent.getString("isUnicorn"));
            newEnt.put("isUnicorn", ent.getString("isUnicorn"));
            newEnt.put("isGazelle", ent.getString("isGazelle"));

            newEnt.put("isStock", ent.getString("isStock"));
            newEnt.put("isTaxARate", ent.getString("isTaxARate"));
            newEnt.put("isAbnormally", ent.getString("isAbnormally"));
            newEnt.put("noInvest", ent.getString("noInvest"));
            newEnt.put("noAnnualInvest", ent.getString("noAnnualInvest"));
            newEnt.put("noLinkedin", ent.getString("noLinkedin"));
            newEnt.put("noAnnualReport", ent.getString("noAnnualReport"));
            newEnt.put("noWeibo", ent.getString("noWeibo"));
            newEnt.put("noFiscalAgent", ent.getString("noFiscalAgent"));
            newEnt.put("noMobile", ent.getString("noMobile"));
            newEnt.put("noTel", ent.getString("noTel"));
            newEnt.put("noEmail", ent.getString("noEmail"));
            newEnt.put("noDomain", ent.getString("noDomain"));
            newEnt.put("noICP", ent.getString("noICP"));
            newEnt.put("noOnlineService", ent.getString("noOnlineService"));
            newEnt.put("noHttps", ent.getString("noHttps"));
            newEnt.put("noTradeCredit", ent.getString("noTradeCredit"));
            newEnt.put("noCert", ent.getString("noCert"));
            newEnt.put("noWeMedia", ent.getString("noWeMedia"));
            newEnt.put("noWeiboOrg", ent.getString("noWeiboOrg"));
            newEnt.put("noNews", ent.getString("noNews"));
            newEnt.put("noWechat", ent.getString("noWechat"));

            newEnt.put("noPrmt", ent.getString("noPrmt"));
            newEnt.put("noGoods", ent.getString("noGoods"));
            newEnt.put("noApp", ent.getString("noApp"));
            newEnt.put("noBrand", ent.getString("noBrand"));
            newEnt.put("noPunishment", ent.getString("noPunishment"));
            newEnt.put("noEquityPledge", ent.getString("noEquityPledge"));
            newEnt.put("noIllegal", ent.getString("noIllegal"));
            newEnt.put("noMortgage", ent.getString("noMortgage"));
            newEnt.put("noIPRPledge", ent.getString("noIPRPledge"));
            newEnt.put("noJudicialAid", ent.getString("noJudicialAid"));
            newEnt.put("noJudicialPaper", ent.getString("noJudicialPaper"));
            newEnt.put("noCourtNotice", ent.getString("noCourtNotice"));
            newEnt.put("noTrademark", ent.getString("noTrademark"));
            newEnt.put("noPatent", ent.getString("noPatent"));
            newEnt.put("noSr", ent.getString("noSr"));
            newEnt.put("noCr", ent.getString("noCr"));
        }
        array.add(newEnt);
        newJson.put("ent", array);
        return newJson;
    }

    public static void insertMangoByJson(JSONObject json) {

        Set<String> keys = json.keySet();
        try {
            for (String key : keys) {
                //新媒体
                if (TableName.MANGO_ENT_NEW_MEDIA.equals(key)) {
                    JSONArray mediaArray = json.getJSONArray(key);
                    for (int i = 0; i < mediaArray.size(); i++) {
                        JSONObject media = mediaArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(media.getString("name"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_NEW_MEDIA);

                            JSONObject queryJson = new JSONObject();
                            queryJson.put("name", media.getString("name"));
                            queryJson.put("entId", media.getString("entId"));

                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", media.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //ent
//                else if (TableName.MANGO_ENT.equals(key)) {
//                    JSONArray entArray = json.getJSONArray(key);
//                    for (int i = 0; i < entArray.size(); i++) {
//                        JSONObject ent = entArray.getJSONObject(i);
//                        if (!StringUtils.isEmpty(ent.getString("entId"))) {
//                            Map<String, Object> map = new HashMap<String, Object>();
//                            map.put("tableName", TableName.MANGO_ENT_COUNT);
//
//                            JSONObject queryJson = new JSONObject();
//                            queryJson.put("_id", ent.getString("_id"));
//
//                            map.put("queryJson", queryJson.toJSONString());
//                            JSONObject mangoJson = MongoUtils.select(map);
//                            if (mangoJson == null) {
//                                mangoJson = new JSONObject();
//                                mangoJson.put("entId", ent.getString("_id"));
////                                mangoJson.put("_id",ent.getString("_id"));
//                            }
////                            entReset(mangoJson,ent);
//                            entCountReset(mangoJson, ent);
//                            map.put("updateJson", mangoJson.toJSONString());
//                            MongoUtils.upsert(map);
//                        }
//                    }
//                }

                //行政许可
                else if (TableName.MANGO_ENT_LICENCE.equals(key)) {
                    JSONArray licenceArray = json.getJSONArray(key);
                    for (int i = 0; i < licenceArray.size(); i++) {
                        JSONObject licence = licenceArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(licence.getString("licNo"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_LICENCE);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("licNo", licence.getString("licNo"));
                            queryJson.put("entId", licence.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", licence.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //联系人
                else if (TableName.MANGO_ENT_CONTACTS.equals(key)) {
                    JSONArray contactArray = json.getJSONArray(key);
                    for (int i = 0; i < contactArray.size(); i++) {
                        JSONObject contact = contactArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(contact.getString("content"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_CONTACTS);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("content", contact.getString("content"));
                            queryJson.put("entId", contact.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());

                            //查询表 source字段
                            JSONObject mangoJson = MongoUtils.select(map);
                            JSONArray sourceArray = new JSONArray();
                            HashSet<String> set = new HashSet<String>();
                            if (mangoJson.getJSONArray("source") != null) {
                                for (int j = 0; j < mangoJson.getJSONArray("source").size(); j++) {
                                    if (!set.contains(mangoJson.getJSONArray("source").getJSONObject(j).toJSONString())) {
                                        sourceArray.add(mangoJson.getJSONArray("source").getJSONObject(j));
                                        set.add(mangoJson.getJSONArray("source").getJSONObject(j).toJSONString());
                                    }
                                }
                            }
                            if (contact.getJSONArray("source") != null) {
                                for (int j = 0; j < contact.getJSONArray("source").size(); j++) {
                                    if (!set.contains(contact.getJSONArray("source").getJSONObject(j).toJSONString())) {
                                        sourceArray.add(contact.getJSONArray("source").getJSONObject(j));
                                        set.add(contact.getJSONArray("source").getJSONObject(j).toJSONString());
                                    }
                                }
                            }
                            contact.put("source", sourceArray.toJSONString());

                            map.put("updateJson", contact.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //招聘
                else if (TableName.MANGO_ENT_RECRUIT.equals(key)) {
                    JSONArray recuitArray = json.getJSONArray(key);
                    for (int i = 0; i < recuitArray.size(); i++) {
                        JSONObject recruit = recuitArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(recruit.getString("title"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_RECRUIT);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("title", recruit.getString("title"));
                            queryJson.put("entId", recruit.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", recruit.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }

                }
                //网站
                else if (TableName.MANGO_ENT_WEBSITE.equals(key)) {
                    JSONArray websiteArray = json.getJSONArray(key);

                    for (int i = 0; i < websiteArray.size(); i++) {
                        JSONObject website = websiteArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(website.getString("title"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_WEBSITE);
                            JSONObject queryJson = new JSONObject();

                            queryJson.put("entId", website.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            //查询表 ipList，icpList
                            JSONObject mangoJson = MongoUtils.select(map);
                            JSONArray ipListArray = new JSONArray();
                            HashSet<String> set = new HashSet<String>();
                            if (mangoJson.getJSONArray("ipList") != null) {
                                for (int j = 0; j < mangoJson.getJSONArray("ipList").size(); j++) {
                                    if (!set.contains(mangoJson.getJSONArray("ipList").getJSONObject(j).toJSONString())) {
                                        ipListArray.add(mangoJson.getJSONArray("ipList").getJSONObject(j));
                                        set.add(mangoJson.getJSONArray("ipList").getJSONObject(j).toJSONString());
                                    }

                                }
                            }
                            if (website.getJSONArray("ipList") != null) {
                                for (int j = 0; j < website.getJSONArray("source").size(); j++) {
                                    if (!set.contains(website.getJSONArray("source").getJSONObject(j).toJSONString())) {
                                        ipListArray.add(website.getJSONArray("source").getJSONObject(j));
                                        set.add(website.getJSONArray("source").getJSONObject(j).toJSONString());
                                    }
                                }
                            }
                            website.put("ipList", ipListArray.toJSONString());

                            JSONArray icpListArray = new JSONArray();
                            if (mangoJson.getJSONArray("icpList") != null) {
                                for (int j = 0; j < mangoJson.getJSONArray("icpList").size(); j++) {
                                    if (!set.contains(mangoJson.getJSONArray("icpList").getJSONObject(j).toJSONString())) {
                                        icpListArray.add(mangoJson.getJSONArray("icpList").getJSONObject(j));
                                        set.add(mangoJson.getJSONArray("icpList").getJSONObject(j).toJSONString());
                                    }

                                }
                            }
                            if (website.getJSONArray("icpList") != null) {
                                for (int j = 0; j < website.getJSONArray("icpList").size(); j++) {
                                    if (!set.contains(website.getJSONArray("icpList").getJSONObject(j).toJSONString())) {
                                        icpListArray.add(website.getJSONArray("icpList").getJSONObject(j));
                                        set.add(website.getJSONArray("icpList").getJSONObject(j).toJSONString());
                                    }
                                }
                            }
                            website.put("icpList", icpListArray.toJSONString());

                            map.put("updateJson", website.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }

                }
                //证书
                else if (TableName.MANGO_ENT_CERT.equals(key)) {
                    JSONArray certArray = json.getJSONArray(key);
                    for (int i = 0; i < certArray.size(); i++) {
                        JSONObject cert = certArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(cert.getString("certNo"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_CERT);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("certNo", cert.getString("certNo"));
                            queryJson.put("entId", cert.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", cert.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //新闻
                else if (TableName.MANGO_ENT_NEWS.equals(key)) {
                    JSONArray newArray = json.getJSONArray(key);
                    for (int i = 0; i < newArray.size(); i++) {
                        JSONObject news = newArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(news.getString("title"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_NEWS);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("title", news.getString("title"));
                            queryJson.put("entId", news.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", news.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //推广
                else if (TableName.MANGO_ENT_PROMOTION.equals(key)) {
                    JSONArray promotionArray = json.getJSONArray(key);
                    for (int i = 0; i < promotionArray.size(); i++) {
                        JSONObject promotion = promotionArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(promotion.getString("content"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_NEWS);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("content", promotion.getString("content"));
                            queryJson.put("entId", promotion.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", promotion.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //商品
                else if (TableName.MANGO_ENT_GOODS.equals(key)) {
                    JSONArray goodsArray = json.getJSONArray(key);
                    for (int i = 0; i < goodsArray.size(); i++) {
                        JSONObject goods = goodsArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(goods.getString("goodsName"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_GOODS);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("goodsName", goods.getString("goodsName"));
                            queryJson.put("entId", goods.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", goods.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //电商店铺
                else if (TableName.MANGO_ENT_ECOMMERCE.equals(key)) {
                    JSONArray ecommerceArray = json.getJSONArray(key);
                    for (int i = 0; i < ecommerceArray.size(); i++) {
                        JSONObject ecommerce = ecommerceArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(ecommerce.getString("shopName"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_ECOMMERCE);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("shopName", ecommerce.getString("shopName"));
                            queryJson.put("entId", ecommerce.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", ecommerce.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //app
                else if (TableName.MANGO_ENT_APPS.equals(key)) {
                    JSONArray appsArray = json.getJSONArray(key);
                    for (int i = 0; i < appsArray.size(); i++) {
                        JSONObject app = appsArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(app.getString("appName"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_APPS);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("appName", app.getString("appName"));
                            queryJson.put("entId", app.getString("entId"));
                            queryJson.put("appStores", app.getString("appStores"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", app.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //品牌
                else if (TableName.MANGO_ENT_BRAND.equals(key)) {
                    JSONArray brandArray = json.getJSONArray(key);
                    for (int i = 0; i < brandArray.size(); i++) {
                        JSONObject brand = brandArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(brand.getString("name"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_BRAND);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("name", brand.getString("name"));
                            queryJson.put("entId", brand.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", brand.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //经营异常
                else if (TableName.MANGO_ENT_ABNORMAL_OPT.equals(key)) {
                    JSONArray abnormalArray = json.getJSONArray(key);
                    for (int i = 0; i < abnormalArray.size(); i++) {
                        JSONObject abnormal = abnormalArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(abnormal.getString("abntime"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_ABNORMAL_OPT);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("abntime", abnormal.getString("abntime"));
                            queryJson.put("speCauseCN", abnormal.getString("speCauseCN"));
                            queryJson.put("entId", abnormal.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", abnormal.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }

                }
                //行政处罚
                else if (TableName.MANGO_ENT_PUNISHMENT.equals(key)) {
                    JSONArray punishmentArray = json.getJSONArray(key);
                    for (int i = 0; i < punishmentArray.size(); i++) {
                        JSONObject punishment = punishmentArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(punishment.getString("caseId"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_PUNISHMENT);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("decNo", punishment.getString("decNo"));
                            queryJson.put("publicDate", punishment.getString("publicDate"));
                            queryJson.put("entId", punishment.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", punishment.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //商标
                else if (TableName.MANGO_ENT_TRADEMARK.equals(key)) {
                    JSONArray trademarkArray = json.getJSONArray(key);
                    for (int i = 0; i < trademarkArray.size(); i++) {
                        JSONObject trademark = trademarkArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(trademark.getString("tmId"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_TRADEMARK);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("tmId", trademark.getString("tmId"));
                            queryJson.put("entId", trademark.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", trademark.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //专利
                else if (TableName.MANGO_ENT_PATENT.equals(key)) {
                    //有效状态商标数量
                    JSONArray patentArray = json.getJSONArray(key);
                    for (int i = 0; i < patentArray.size(); i++) {
                        JSONObject patent = patentArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(patent.getString("name"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_PATENT);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("pubNo", patent.getString("pubNo"));
                            queryJson.put("entId", patent.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", patent.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //软著
                else if (TableName.MANGO_ENT_SOFTWARE.equals(key)) {
                    //Integer srCount;//软著数量
                    JSONArray softwareArray = json.getJSONArray(key);
                    for (int i = 0; i < softwareArray.size(); i++) {
                        JSONObject software = softwareArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(software.getString("name"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_PATENT);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("name", software.getString("name"));
                            queryJson.put("entId", software.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", software.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //对外投资
                else if (TableName.MANGO_ENT_INVESTCOMPANY.equals(key)) {
                    JSONArray investArray = json.getJSONArray(key);
                    for (int i = 0; i < investArray.size(); i++) {
                        JSONObject invest = investArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(invest.getString("brunch"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_INVESTCOMPANY);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("brunch", invest.getString("brunch"));
                            queryJson.put("entId", invest.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", invest.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //年报
                else if (TableName.MANGO_ENT_ANNUAL_REPORT.equals(key)) {
                    JSONArray annualReportArray = json.getJSONArray(key);
                    for (int i = 0; i < annualReportArray.size(); i++) {
                        JSONObject annual = annualReportArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(annual.getString("pripId"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_ANNUAL_REPORT);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("anCheYear", annual.getString("anCheYear"));
                            queryJson.put("entId", annual.getString("entId"));

                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", annual.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //官网
//                else if (TableName.MANGO_ENT_DOMAIN.equals(key)) {
//                    JSONArray domainArray = json.getJSONArray(key);
//                    for (int i = 0; i < domainArray.size(); i++) {
//                        JSONObject domain = domainArray.getJSONObject(i);
//                        if (!StringUtils.isEmpty(domain.getString("domain"))) {
//                            Map<String, Object> map = new HashMap<String, Object>();
//                            map.put("tableName", TableName.MANGO_ENT_DOMAIN);
//                            JSONObject queryJson = new JSONObject();
//                            queryJson.put("domain", domain.getString("domain"));
//                            queryJson.put("entId", domain.getString("entId"));
//                            map.put("queryJson", queryJson.toJSONString());
//                            map.put("updateJson", domain.toJSONString());
//                            MongoUtils.upsert(map);
//                        }
//                    }
//                }
                //股权出资
                else if (TableName.MANGO_ENT_EQUITY_PLEDGED.equals(key)) {
                    JSONArray equityArray = json.getJSONArray(key);
                    for (int i = 0; i < equityArray.size(); i++) {
                        JSONObject equity = equityArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(equity.getString("equityNo"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_EQUITY_PLEDGED);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("equityNo", equity.getString("equityNo"));
                            queryJson.put("entId", equity.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", equity.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //变动
                else if (TableName.MANGO_ENT_ALTERINFO.equals(key)) {
                    JSONArray alterinfoArray = json.getJSONArray(key);
                    for (int i = 0; i < alterinfoArray.size(); i++) {
                        JSONObject alterinfo = alterinfoArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(alterinfo.getString("altItemCN"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_ALTERINFO);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("altItemCN", alterinfo.getString("altItemCN"));
                            queryJson.put("entId", alterinfo.getString("entId"));
                            queryJson.put("altDate", alterinfo.getString("altDate"));
                            queryJson.put("altBe", alterinfo.getString("altBe"));
                            queryJson.put("altAf", alterinfo.getString("altAf"));

                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", alterinfo.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //融资历史
                else if (TableName.MANGO_ENT_FUNDING_EVENT.equals(key)) {
                    JSONArray fundingArray = json.getJSONArray(key);
                    for (int i = 0; i < fundingArray.size(); i++) {
                        JSONObject funding = fundingArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(funding.getString("pubDate"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_FUNDING_EVENT);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("pubDate", funding.getString("pubDate"));
                            queryJson.put("entId", funding.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", funding.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //发展历史
//                else if (TableName.MANGO_ENT_HISTORY.equals(key)) {
//                    JSONArray historyArray = json.getJSONArray(key);
//                    for (int i = 0; i < historyArray.size(); i++) {
//                        JSONObject history = historyArray.getJSONObject(i);
//                        if (!StringUtils.isEmpty(history.getString("title"))) {
//                            Map<String, Object> map = new HashMap<String, Object>();
//                            map.put("tableName", TableName.MANGO_ENT_HISTORY);
//                            JSONObject queryJson = new JSONObject();
//                            queryJson.put("title", history.getString("title"));
//                            queryJson.put("entId", history.getString("entId"));
//                            map.put("queryJson", queryJson.toJSONString());
//                            map.put("updateJson", history.toJSONString());
//                            MongoUtils.upsert(map);
//                        }
//                    }
//                }
                //关键人
                else if (TableName.MANGO_ENT_KEY_PERSON.equals(key)) {
                    JSONArray keyPersonArray = json.getJSONArray(key);
                    for (int i = 0; i < keyPersonArray.size(); i++) {
                        JSONObject keyPerson = keyPersonArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(keyPerson.getString("name"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_KEY_PERSON);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("name", keyPerson.getString("name"));
                            queryJson.put("positionCN", keyPerson.getString("positionCN"));
                            queryJson.put("entId", keyPerson.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", keyPerson.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
                //诉讼
//                else if (TableName.MANGO_ENT_LAWSUIT.equals(key)) {
//                    JSONArray lawsuitArray = json.getJSONArray(key);
//                    for (int i = 0; i < lawsuitArray.size(); i++) {
//                        JSONObject lawsuit = lawsuitArray.getJSONObject(i);
//                        if (!StringUtils.isEmpty(lawsuit.getString("caseNo"))) {
//                            Map<String, Object> map = new HashMap<String, Object>();
//                            map.put("tableName", TableName.MANGO_ENT_LAWSUIT);
//                            JSONObject queryJson = new JSONObject();
//                            queryJson.put("caseNo", lawsuit.getString("caseNo"));
//                            queryJson.put("entId", lawsuit.getString("entId"));
//                            queryJson.put("date", lawsuit.getString("date"));
//                            map.put("queryJson", queryJson.toJSONString());
//                            map.put("updateJson", lawsuit.toJSONString());
//                            MongoUtils.upsert(map);
//                        }
//                    }
//                }
                //股东
                else if (TableName.MANGO_ENT_SHAREHOLDER.equals(key)) {
                    JSONArray shareArray = json.getJSONArray(key);
                    for (int i = 0; i < shareArray.size(); i++) {
                        JSONObject share = shareArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(share.getString("inv"))) {
                            Map<String, Object> map = new HashMap<String, Object>();
                            map.put("tableName", TableName.MANGO_ENT_SHAREHOLDER);
                            JSONObject queryJson = new JSONObject();
                            queryJson.put("inv", share.getString("inv"));
                            queryJson.put("entId", share.getString("entId"));
                            map.put("queryJson", queryJson.toJSONString());
                            map.put("updateJson", share.toJSONString());
                            MongoUtils.upsert(map);
                        }
                    }
                }
            }
        } catch (Exception e) {
        }
    }

    private static void entCountReset(JSONObject oldEnt, JSONObject currentEnt) {
        //  # 统计字段

        String count[] = {"branchCount", "licCount", "insuredCount", "contactCount", "postCount", "recruitCount", "iCPCount", "lTMPostCount", "lTMJobCount", "certCount", "weiboOrgCount", "lMNewsCount", "prmtKeyCount",
                "prmtCompCount", "itemCount", "goodsCount", "appCount", "brandCount", "abnormalCount", "punishmentCount", "tmValidCount", "tmInvalidCount", "patentCount", "lYPatentCount", "lYPatentPublicCount",
                "srCount", "crCount", "wechatCount", "financingCount", "appDownloadCount", "certThisYearCount", "prmtLinksCount", "ecShopCount"};

        for (String feild : count) {
            if (!StringUtils.isEmpty(currentEnt.getString(feild))) {
                if (!StringUtils.isEmpty(oldEnt.getString(feild))) {
                    oldEnt.put(feild, Integer.parseInt(currentEnt.getString(feild)) + Integer.parseInt(oldEnt.getString(feild)));
                } else {
                    oldEnt.put(feild, currentEnt.getString(feild));
                }
            }
        }

        String iss[] = {

                "domainAgeCount", "appScoreAvg", "salaryAvg", "postUpdateTimeAvg", "errorRateAvg", "respTimeAvg",

                "isHighTech", "isTop500", "isUnicorn", "isGazelle", "isStock", "isTaxARate", "isAbnormally", "noLicence", "noInvest", "noAnnualInvest", "noLinkedin", "noAnnualReport", "noWeibo",
                "noFiscalAgent", "noMobile", "noTel", "noEmail", "noDomain", "noICP", "noOnlineService", "noHttps", "noBidding", "noTradeCredit", "noCert", "noWeMedia",
                "noWeiboOrg", "noNews", "noWechat", "noPrmt", "noGoods", "noApp", "noBrand", "noPunishment", "noEquityPledge", "noIllegal", "noMortgage", "noIPRPledge", "noJudicialAid", "noJudicialPaper",
                "noCourtNotice", "noTrademark", "noPatent", "noSr", "noCr"};

        for (String is : iss) {
            if (!StringUtils.isEmpty(currentEnt.getString(is))) {
                oldEnt.put(is, currentEnt.getString(is));
            }
        }
    }

    public static void entReset(JSONObject oldEnt, JSONObject currentEnt) {
        if (!StringUtils.isEmpty(currentEnt.getString("uniscId"))) {
            oldEnt.put("uniscId", currentEnt.getString("uniscId"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("corpStatus"))) {
            oldEnt.put("corpStatus", currentEnt.getString("corpStatus"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("corpStatusString"))) {
            oldEnt.put("corpStatusString", currentEnt.getString("corpStatusString"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("entType"))) {
            oldEnt.put("entType", currentEnt.getString("entType"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("entTypeString"))) {
            oldEnt.put("entTypeString", currentEnt.getString("entTypeString"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("entTypeCN"))) {
            oldEnt.put("entTypeCN", currentEnt.getString("entTypeCN"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("estDate"))) {
            oldEnt.put("estDate", currentEnt.getString("estDate"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("apprDate"))) {
            oldEnt.put("apprDate", currentEnt.getString("apprDate"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("regState"))) {
            oldEnt.put("regState", currentEnt.getString("regState"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("regStateCN"))) {
            oldEnt.put("regStateCN", currentEnt.getString("regStateCN"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("regOrgCn"))) {
            oldEnt.put("regOrgCn", currentEnt.getString("regOrgCn"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("regOrg"))) {
            oldEnt.put("regOrg", currentEnt.getString("regOrg"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("regCapCurCN"))) {
            oldEnt.put("regCapCurCN", currentEnt.getString("regCapCurCN"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("regCaption"))) {
            oldEnt.put("regCaption", currentEnt.getString("regCaption"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("regCap"))) {
            oldEnt.put("regCap", currentEnt.getString("regCap"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("regNo"))) {
            oldEnt.put("regNo", currentEnt.getString("regNo"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("taxNo"))) {
            oldEnt.put("taxNo", currentEnt.getString("taxNo"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("groupNo"))) {
            oldEnt.put("groupNo", currentEnt.getString("groupNo"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("nodeNum"))) {
            oldEnt.put("nodeNum", currentEnt.getString("nodeNum"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("legalName"))) {
            oldEnt.put("legalName", currentEnt.getString("legalName"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("opFrom"))) {
            oldEnt.put("opFrom", currentEnt.getString("opFrom"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("opTo"))) {
            oldEnt.put("opTo", currentEnt.getString("opTo"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("opScope"))) {
            oldEnt.put("opScope", currentEnt.getString("opScope"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("dom"))) {
            oldEnt.put("dom", currentEnt.getString("dom"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("industryPhy"))) {
            oldEnt.put("industryPhy", currentEnt.getString("industryPhy"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("eGrpShform"))) {
            oldEnt.put("eGrpShform", currentEnt.getString("eGrpShform"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("eGrpName"))) {
            oldEnt.put("eGrpName", currentEnt.getString("eGrpName"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("ePubGroup"))) {
            oldEnt.put("ePubGroup", currentEnt.getString("ePubGroup"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("entTypeForAnnualReport"))) {
            oldEnt.put("entTypeForAnnualReport", currentEnt.getString("entTypeForAnnualReport"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("entTypeForQuery"))) {
            oldEnt.put("entTypeForQuery", currentEnt.getString("entTypeForQuery"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("entTypeForQuery"))) {
            oldEnt.put("entTypeForQuery", currentEnt.getString("entTypeForQuery"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("linkmanName"))) {
            oldEnt.put("linkmanName", currentEnt.getString("linkmanName"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("linkmanCerNo"))) {
            oldEnt.put("linkmanCerNo", currentEnt.getString("linkmanCerNo"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("linkmanPhone"))) {
            oldEnt.put("linkmanPhone", currentEnt.getString("linkmanPhone"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("preEntType"))) {
            oldEnt.put("preEntType", currentEnt.getString("preEntType"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("employeesMin"))) {
            oldEnt.put("employeesMin", currentEnt.getString("employeesMin"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("employeesMax"))) {
            oldEnt.put("employeesMax", currentEnt.getString("employeesMax"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("regCity"))) {
            oldEnt.put("regCity", currentEnt.getString("regCity"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("regProv"))) {
            oldEnt.put("regProv", currentEnt.getString("regProv"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("regDistrict"))) {
            oldEnt.put("regDistrict", currentEnt.getString("regDistrict"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("annualTurnoverMin"))) {
            oldEnt.put("annualTurnoverMin", currentEnt.getString("annualTurnoverMin"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("annualTurnoverMax"))) {
            oldEnt.put("annualTurnoverMax", currentEnt.getString("annualTurnoverMax"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("source"))) {
            oldEnt.put("source", currentEnt.getString("source"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("sourceUrl"))) {
            oldEnt.put("sourceUrl", currentEnt.getString("sourceUrl"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("compForm"))) {
            oldEnt.put("compForm", currentEnt.getString("compForm"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("compFormCN"))) {
            oldEnt.put("compFormCN", currentEnt.getString("compFormCN"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("busExceptCount"))) {
            oldEnt.put("busExceptCount", currentEnt.getString("busExceptCount"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("illCount"))) {
            oldEnt.put("illCount", currentEnt.getString("illCount"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("notFound"))) {
            oldEnt.put("notFound", currentEnt.getString("notFound"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("historyName"))) {
            oldEnt.put("historyName", currentEnt.getString("historyName"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("pripid"))) {
            oldEnt.put("pripid", currentEnt.getString("pripid"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("spider"))) {
            oldEnt.put("spider", currentEnt.getString("spider"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("products"))) {
            oldEnt.put("products", currentEnt.getString("products"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("desc"))) {
            oldEnt.put("desc", currentEnt.getString("desc"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("category"))) {
            oldEnt.put("category", currentEnt.getString("category"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("domain"))) {
            oldEnt.put("domain", currentEnt.getString("domain"));
        }
        if (!StringUtils.isEmpty(currentEnt.getString("location"))) {
            oldEnt.put("location", currentEnt.getString("location"));
        }
    }

    public static JSONObject deleteNull(JSONObject json) {
        JSONObject newjson = new JSONObject();
        for (String key : json.keySet()) {

            JSONArray newArray = new JSONArray();
            JSONArray array = json.getJSONArray(key);
            for (int i = 0; i < array.size(); i++) {
                JSONObject newJson = new JSONObject();
                JSONObject cjson = array.getJSONObject(i);
                for (String key1 : cjson.keySet()) {
                    if (!StringUtils.isEmpty(cjson.getString(key1)) && StringUtils.isEmpty(cjson.getString(key1).trim())) {
                        newJson.put(key1, cjson.getString(key1));
                    }
                }
                newArray.add(newJson);
            }
            newjson.put(key, newArray);
        }
        return newjson;
    }

    private static void setDefault(EntItem entItem) {

        entItem.setIsTaxARate(0);
        entItem.setIsAbnormally(0);
        entItem.setNoAbnomalyOpt(1);
        entItem.setNoAnnualReport(1);
        entItem.setNoApp(1);
        entItem.setNoBidding(1);
        entItem.setNoBrand(1);
        entItem.setNoCert(1);
        entItem.setNoCr(1);
        entItem.setNoMobile(1);
        entItem.setNoTel(1);
        entItem.setNoEmail(1);
        entItem.setNoCourtNotice(1);
        entItem.setNoJudicialPaper(1);
        entItem.setNoEquityPledge(1);
        entItem.setNoFiscalAgent(1);
        entItem.setNoGoods(1);
        entItem.setIsGazelle(0);
        entItem.setIsUnicorn(0);
        entItem.setIsHighTech(0);
        entItem.setNoInvest(1);
        entItem.setNoLicence(1);
        entItem.setIsStock(0);
        entItem.setNoWeMedia(1);
        entItem.setNoWechat(1);
        entItem.setNoWeibo(1);
        entItem.setNoNews(1);
        entItem.setNoPatent(1);
        entItem.setNoIllegal(1);
        entItem.setNoPunishment(1);
        entItem.setNoSr(1);
        entItem.setIsTop500(0);
        entItem.setNoTrademark(1);
        entItem.setNoICP(1);
        entItem.setNoDomain(1);
        entItem.setNoOnlineService(1);
        entItem.setNoHttps(1);
    }

    public static JSONObject reset(JSONObject currentJson, JSONObject oldJson) {
        JSONObject json = oldJson;

        try {
            //将currentJson中的内容设置到json里面，然后重新统计
            for (String key : currentJson.keySet()) {
                //ent
                if (TableName.MANGO_ENT.equals(key)) {
                    JSONObject oldEnt = json.getJSONArray(key).getJSONObject(0);
                    JSONObject currentEnt = currentJson.getJSONArray(key).getJSONObject(0);
                    entReset(oldEnt, currentEnt);
                } else if (TableName.MANGO_ENT_CONTACTS.equals(key)) {
                    if (json.getJSONArray(key) == null) {
                        json.put(key, currentJson.getJSONArray(key));
                    } else {
                        JSONArray a = json.getJSONArray(key);
                        HashSet<String> set = new HashSet<String>();
                        for (int i = 0; i < a.size(); i++) {
                            set.add(a.getJSONObject(i).toJSONString());
                        }
                        for (int j = 0; j < currentJson.getJSONArray(key).size(); j++) {
                            if (!set.contains(currentJson.getJSONArray(key).getJSONObject(j).toJSONString())) {
                                a.add(currentJson.getJSONArray(key).getJSONObject(j));
                            }
                        }
                        json.put(key, a);
                    }
                }
                //其他关联表
                else {
                    if (json.getJSONArray(key) == null) {
                        json.put(key, currentJson.getJSONArray(key));
                    } else {
                        JSONArray a = json.getJSONArray(key);
                        HashSet<String> set = new HashSet<String>();
                        for (int i = 0; i < a.size(); i++) {
                            set.add(a.getJSONObject(i).toJSONString());
                        }
                        for (int j = 0; j < currentJson.getJSONArray(key).size(); j++) {
                            if (!set.contains(currentJson.getJSONArray(key).getJSONObject(j).toJSONString())) {
                                a.add(currentJson.getJSONArray(key).getJSONObject(j));
                            }
                        }
                        json.put(key, a);
                    }
                }
            }

            //重新统计
            JSONObject entJson = json.getJSONArray("ent").getJSONObject(0);
            EntItem entItem = new EntItem();
            ReflectField.setField(entJson, entItem);
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");


            Date date = new Date();
            for (String key : json.keySet()) {
                //新媒体
                if (TableName.MANGO_ENT_NEW_MEDIA.equals(key)) {
                    JSONArray mediaArray = json.getJSONArray(key);
                    int wechatCount = 0;
                    int weiboOrgCount = 0;

                    for (int i = 0; i < mediaArray.size(); i++) {
                        JSONObject media = mediaArray.getJSONObject(i);
                        if ("1".equals(media.getString("type"))) {
                            wechatCount++;
                            entItem.setNoWechat(Yes);
                        }
                        if ("2".equals(media.getString("type"))) {
                            weiboOrgCount++;
                            entItem.setNoWeibo(Yes);
                        }
                    }
                    entItem.setWeiboOrgCount(weiboOrgCount);
                    entItem.setWechatCount(wechatCount);
                    entItem.setNoWeMedia(Yes);
                }

                //行政许可
                else if (TableName.MANGO_ENT_LICENCE.equals(key)) {
                    JSONArray licenceArray = json.getJSONArray(key);
                    int licCount = licenceArray.size();
                    entItem.setLicCount(licCount);
                    for (int i = 0; i < licenceArray.size(); i++) {
                        if (!StringUtils.isEmpty(licenceArray.getJSONObject(i).getString("valTo")) && format.parse(licenceArray.getJSONObject(i).getString("valTo")).getTime() > date.getTime()) {
                            entItem.setNoLicence(Yes);
                        }
                    }
                }
                //联系人
                else if (TableName.MANGO_ENT_CONTACTS.equals(key)) {
                    JSONArray contactArray = json.getJSONArray(key);
                    int contactCount = contactArray.size();
                    entItem.setContactCount(contactCount);
                    for (int i = 0; i < contactArray.size(); i++) {
                        if ("1".equals(contactArray.getJSONObject(i).getString("type"))) {
                            entItem.setNoMobile(Yes);
                        }
                        if ("2".equals(contactArray.getJSONObject(i).getString("type"))) {
                            entItem.setNoTel(Yes);
                        }
                        if ("3".equals(contactArray.getJSONObject(i).getString("type"))) {
                            entItem.setNoEmail(Yes);
                        }
                    }
                }
                //招聘
                else if (TableName.MANGO_ENT_RECRUIT.equals(key)) {
                    JSONArray recuitArray = json.getJSONArray(key);
                    int postCount = 0;//当前招聘岗位数
                    int recruitCount = 0;//当前招聘人数
                    int lTMPostCount = 0;////近三个月招聘岗位数
                    int lTMJobCount = 0;//近三个月招聘总人数
                    Long postUpdateTimeAvg = 0l;//岗位平均更新频率
                    Double salary = 0.0;
                    int salaryNum = 0;
                    entItem.setRecruitCount(recuitArray.size());
                    HashSet<String> set = new HashSet<String>();
                    HashSet<String> ltmpostSet = new HashSet<String>();
                    for (int i = 0; i < recuitArray.size(); i++) {
                        JSONObject recuitJson = recuitArray.getJSONObject(i);
                        set.add(recuitJson.getString("title"));
                        if (!StringUtils.isEmpty(recuitJson.getString("rdate"))) {
                            String rdateStr = recuitJson.getString("rdate");
                            Date rdate = format.parse(rdateStr);
                            if (date.getTime() - rdate.getTime() < 90 * 24 * 3600000l) {
                                lTMJobCount++;
                                ltmpostSet.add(recuitJson.getString("title"));
                            }
                            if (rdate.getTime() > postUpdateTimeAvg) {
                                postUpdateTimeAvg = rdate.getTime();
                            }
                        }
                        if (!StringUtils.isEmpty(recuitJson.getString("salaryFrom")) && !StringUtils.isEmpty(recuitJson.getString("salaryTo"))) {
                            salary = salary + (Integer.parseInt(recuitJson.getString("salaryFrom")) + Integer.parseInt(recuitJson.getString("salaryTo"))) / 2;
                            salaryNum++;
                        }
                    }
                    entItem.setlTMPostCount(ltmpostSet.size());
                    entItem.setlTMJobCount(lTMJobCount);
                    entItem.setPostCount(set.size());
                    entItem.setSalaryAvg(salary / salaryNum);
                    entItem.setPostUpdateTimeAvg(format.format(new Date(postUpdateTimeAvg)));
                }
                //网站
                else if (TableName.MANGO_ENT_WEBSITE.equals(key)) {
                    JSONArray websiteArray = json.getJSONArray(key);
                    int iCPCount = 0;//ICP备案数量
                    if (websiteArray != null && websiteArray.size() > 0 && websiteArray.getJSONObject(0).getJSONArray("icpList") != null) {
                        iCPCount = websiteArray.getJSONObject(0).getJSONArray("icpList").size();
                        entItem.setNoICP(Yes);
                    }
                    entItem.setiCPCount(iCPCount);
                    int maxAge = 0;//域名年龄
                    if (websiteArray != null && websiteArray.size() > 0 && websiteArray.getJSONObject(0).getJSONArray("ipList") != null) {
                        JSONArray array = websiteArray.getJSONObject(0).getJSONArray("ipList");
                        if (array != null && array.size() > 0) {
                            for (int i = 0; i < array.size(); i++) {
                                JSONObject j = array.getJSONObject(i);
                                String regDateStr = j.getString("regDate");
                                Double age = Math.ceil((date.getTime() - format.parse(regDateStr).getTime()) / 365 * 24 * 3600000f);
                                if (age.intValue() > maxAge) {
                                    maxAge = age.intValue();
                                }
                            }
                        }
                    }
                    entItem.setDomainAgeCount(maxAge);

                }
                //证书
                else if (TableName.MANGO_ENT_CERT.equals(key)) {
                    JSONArray certArray = json.getJSONArray(key);
                    int certCount = certArray.size();
                    entItem.setCertCount(certCount);
                    entItem.setNoCert(Yes);
                }
                //新闻
                else if (TableName.MANGO_ENT_NEWS.equals(key)) {
                    JSONArray newArray = json.getJSONArray(key);
                    int lMNewsCount = 0;//近1月新闻数量

                    for (int i = 0; i < newArray.size(); i++) {
                        JSONObject newJson = newArray.getJSONObject(i);

                        if (!StringUtils.isEmpty(newJson.getString("pubDate"))) {
                            String pubDateStr = newJson.getString("pubDate");
                            Date pubDate = format.parse(pubDateStr);
                            if (date.getTime() - pubDate.getTime() < 30 * 24 * 3600000l) {
                                lMNewsCount++;
                            }
                        }
                    }
                    entItem.setlMNewsCount(lMNewsCount);
                    entItem.setNoNews(Yes);
                }
                //推广
                else if (TableName.MANGO_ENT_PROMOTION.equals(key)) {
                    JSONArray promotionArray = json.getJSONArray(key);
                    Integer prmtKeyCount = promotionArray.size();//推广关键词数
                    Integer prmtCompCount = 0;//推广竞品数
                    entItem.setPrmtKeyCount(prmtKeyCount);
                    entItem.setNoPrmt(Yes);
                }
                //商品
                else if (TableName.MANGO_ENT_GOODS.equals(key)) {
                    JSONArray goodsArray = json.getJSONArray(key);

                    Integer goodsCount = 0;//商品数量
                    entItem.setGoodsCount(goodsArray.size());
                    entItem.setNoGoods(Yes);
                }
                //电商店铺
                else if (TableName.MANGO_ENT_ECOMMERCE.equals(key)) {
                    JSONArray ecommerceArray = json.getJSONArray(key);
                    Integer itemCount = 0;//电商店铺商品总数
                    entItem.setGoodsCount(ecommerceArray.size());
                    Integer ecShopCount = 0;//电商店铺数量
                    HashSet<String> shopSet = new HashSet<String>();
                    for (int i = 0; i < ecommerceArray.size(); i++) {
                        if (!StringUtils.isEmpty(ecommerceArray.getJSONObject(i).getString("shopName"))) {
                            shopSet.add(ecommerceArray.getJSONObject(i).getString("appDownloadCount"));
                        }
                    }
                    entItem.setEcShopCount(shopSet.size());
                }
                //app
                else if (TableName.MANGO_ENT_APPS.equals(key)) {
                    JSONArray appsArray = json.getJSONArray(key);
                    entItem.setAppCount(appsArray.size());
                    int downloadCount = 0;
                    double appScore = 0;
                    int num = 0;
                    for (int i = 0; i < appsArray.size(); i++) {
                        if (!StringUtils.isEmpty(appsArray.getJSONObject(i).getString("appDownloadCount"))) {
                            downloadCount = downloadCount + Integer.parseInt(appsArray.getJSONObject(i).getString("appDownloadCount"));
                        }
                        if (!StringUtils.isEmpty(appsArray.getJSONObject(i).getString("appScore"))) {
                            appScore = appScore + Double.parseDouble(appsArray.getJSONObject(i).getString("appScore"));
                            num++;
                        }

                    }
                    entItem.setAppDownloadCount(downloadCount);
                    entItem.setAppScoreAvg(appScore / num);
                    entItem.setNoApp(Yes);
                }
                //品牌
                else if (TableName.MANGO_ENT_BRAND.equals(key)) {
                    JSONArray brandArray = json.getJSONArray(key);
                    entItem.setBrandCount(brandArray.size());
                    entItem.setNoBrand(Yes);
                }
                //经营异常
                else if (TableName.MANGO_ENT_ABNORMAL_OPT.equals(key)) {
                    JSONArray abnormalArray = json.getJSONArray(key);
                    entItem.setAbnormalCount(abnormalArray.size());
                    for (int i = 0; i < abnormalArray.size(); i++) {
                        if (!StringUtils.isEmpty(abnormalArray.getJSONObject(i).getString("remDate"))) {
                            entItem.setIsAbnormally(Yes);
                            break;
                        }
                    }

                }
                //行政处罚
                else if (TableName.MANGO_ENT_PUNISHMENT.equals(key)) {
                    JSONArray punishmentArray = json.getJSONArray(key);
                    entItem.setPunishmentCount(punishmentArray.size());
                    entItem.setNoPunishment(Yes);
                }
                //商标
                else if (TableName.MANGO_ENT_TRADEMARK.equals(key)) {
                    entItem.setNoTrademark(Yes);
                }
                //专利
                else if (TableName.MANGO_ENT_PATENT.equals(key)) {
                    //有效状态商标数量
                    JSONArray patentArray = json.getJSONArray(key);
                    Integer patentCount = 0;//专利数量
                    Integer lYPatentCount = 0;//最近一年申请专利数量
                    Integer lYPatentPublicCount = 0;//最近一年公开专利数量
                    entItem.setPatentCount(patentArray.size());
                    for (int i = 0; i < patentArray.size(); i++) {
                        JSONObject newJson = patentArray.getJSONObject(i);

                        if (!StringUtils.isEmpty(newJson.getString("appDate"))) {
                            String appDateStr = newJson.getString("appDate");
                            Date appDate = format.parse(appDateStr);
                            if (date.getTime() - appDate.getTime() < 365 * 24 * 3600000l) {
                                lYPatentCount++;
                            }
                        }
                        if (!StringUtils.isEmpty(newJson.getString("pubDate"))) {
                            String pubDateStr = newJson.getString("pubDate");
                            Date pubDate = format.parse(pubDateStr);
                            if (date.getTime() - pubDate.getTime() < 365 * 24 * 3600000l) {
                                lYPatentPublicCount++;
                            }
                        }
                    }
                    entItem.setlYPatentCount(lYPatentCount);
                    entItem.setlYPatentPublicCount(lYPatentPublicCount);
                    entItem.setNoPatent(Yes);
                }
                //软著
                else if (TableName.MANGO_ENT_SOFTWARE.equals(key)) {
                    //Integer srCount;//软著数量
                    entItem.setSrCount(json.getJSONArray(key).size());
                    entItem.setNoSr(Yes);
                }
                //资质证书
                else if (TableName.MANGO_ENT_CERT.equals(key)) {
                    Integer certThisYearCount = 0;//本年度获证数量
                    JSONArray certArray = json.getJSONArray(key);
                    for (int i = 0; i < certArray.size(); i++) {
                        JSONObject certJson = certArray.getJSONObject(i);
                        if (!StringUtils.isEmpty(certJson.getString("pubDate"))) {
                            String pubDateStr = certJson.getString("pubDate");
                            Date pubDate = format.parse(pubDateStr);
                            if (date.getTime() - pubDate.getTime() < 365 * 24 * 3600000l) {
                                certThisYearCount++;
                            }
                        }
                    }
                    entItem.setCertThisYearCount(certThisYearCount);
                }
                //推广
                else if (TableName.MANGO_ENT_PROMOTION.equals(key)) {
                    entItem.setPrmtLinksCount(json.getJSONArray(key).size());
                }
                //对外投资
                else if (TableName.MANGO_ENT_INVESTCOMPANY.equals(key)) {
                    entItem.setNoInvest(Yes);
                }
                //年报
                else if (TableName.MANGO_ENT_ANNUAL_REPORT.equals(key)) {
                    entItem.setNoAnnualReport(Yes);
                }

                //股权出资
                else if (TableName.MANGO_ENT_EQUITY_PLEDGED.equals(key)) {
                    entItem.setNoEquityPledge(Yes);
                }
            }

            JSONObject newEntJson = JSONObject.parseObject(JSON.toJSONString(entItem));
            JSONArray newEntArray = new JSONArray();
            newEntArray.add(newEntJson);
            json.put("ent", newEntArray);
        } catch (Exception e) {
            System.out.println(e + " " + json.toJSONString());
        }

        return json;
    }
}