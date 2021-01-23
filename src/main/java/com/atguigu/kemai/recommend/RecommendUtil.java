package com.atguigu.kemai.recommend;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.kemai.utils.Cosine;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class RecommendUtil {

    /**
     * 根据分数计算相应的json结果
     *
     * @param list
     * @param score
     * @return
     */
    public static Map<String, HashSet<String>> getRecommend(ArrayList<JSONObject> list, Integer score) {

        Map<String, HashSet<String>> map = new HashMap<String, HashSet<String>>();

        for (int i = 0; i < list.size(); i++) {
            //拿到第1个json对象
            JSONObject json = list.get(i);

            String entId1 = json.getString("entId");
            HashSet<String> similarList = null;

            if (map.get(entId1) != null) {
                similarList = map.get(entId1);
            } else {
                similarList = new HashSet<String>();
            }

            for (int j = 0; j < list.size(); j++) {
                //拿到第2个json对象
                JSONObject json2 = list.get(j);
                String entId2 = json2.getString("entId");

                //存放指定json计算出的前100个相似度分数超过score的json的集合
                if (similarList.size() >= 300) {
                    break;
                }

                if (!entId1.equals(entId2)) {
                    double finalSimilar = getFinalSimilar(json, json2);     // 计算两个json的相似度值，double

                    //若计算的总分大于score：
                    if (finalSimilar > score) {
                        String s = String.format("%.2f", finalSimilar);
                        similarList.add(entId2 + "|" + s);

                        if (map.get(entId2) == null) {
                            HashSet<String> similarList1 = new HashSet<String>();
                            similarList1.add(entId1 + "|" + s);
                            map.put(entId2, similarList1);
                        } else {
                            map.get(entId2).add(entId1 + "|" + s);
                        }
                    }
                }
            }
            map.put(entId1, similarList);
        }
        return map;
    }

    /**
     * ent
     * ent_growingup
     * ent_top500
     * ent_listed
     * ent_a_taxpayer
     * ent_abnormal_opt
     * ent_cert
     * ent_recruit
     * ent_domain
     */
    private static Double getFinalSimilar(JSONObject json, JSONObject json2) {
        // json1的属性字段
        String scope1 = json.getString("opScope");
        String isHighTech1 = json.getString("isHighTech");
        String isTop5001 = json.getString("isTop500");
        String isUnicorn1 = json.getString("isUnicorn");
        String isGazelle1 = json.getString("isGazelle");
        String isStock1 = json.getString("isStock");
        String isTaxARate1 = json.getString("isTaxARate");
        String isAbnormally1 = json.getString("isAbnormally");
        String noCert1 = json.getString("noCert");
        String entTypeCN1 = json.getString("entTypeCN");
        String regCaption1 = json.getString("regCaption");
        String employeesMin1 = json.getString("employeesMin");
        String employeesMax1 = json.getString("employeesMax");
        String products1 = json.getString("products");

        // json2的属性字段
        String scope2 = json2.getString("opScope");
        String isHighTech2 = json2.getString("isHighTech");
        String isTop5002 = json2.getString("isTop500");
        String isUnicorn2 = json2.getString("isUnicorn");
        String isGazelle2 = json2.getString("isGazelle");
        String isStock2 = json2.getString("isStock");
        String isTaxARate2 = json2.getString("isTaxARate");
        String isAbnormally2 = json2.getString("isAbnormally");
        String noCert2 = json2.getString("noCert");
        String entTypeCN2 = json2.getString("entTypeCN");
        String regCaption2 = json2.getString("regCaption");
        String employeesMin2 = json2.getString("employeesMin");
        String employeesMax2 = json2.getString("employeesMax");
        String products2 = json2.getString("products");

        /**
         * 员工人数
         */
        double employees = 0;
        if (!StringUtils.isEmpty(employeesMin1) && !StringUtils.isEmpty(employeesMin2) &&
                !StringUtils.isEmpty(employeesMax1) && !StringUtils.isEmpty(employeesMax2)) {

            Integer dreg1 = Integer.parseInt(employeesMin1) + Integer.parseInt(employeesMax1);
            int l1 = dreg1.toString().length();

            Integer dreg2 = Integer.parseInt(employeesMin2) + Integer.parseInt(employeesMax2);
            int l2 = dreg2.toString().length();

            int result = Math.abs(l1 - l2);
            if (result == 0) {
                employees = 1;
            } else {
                employees = 1.0 / result;
            }
        }

        /**
         * 经营范围（余玹相似度）
         */
        double scope = Cosine.getSimilarity(scope1, scope2);
        double products = 0;
        if (!StringUtils.isEmpty(products1) && !StringUtils.isEmpty(products2)) {
            products = Cosine.getSimilarity(products1, products2);
        }

        /**
         * 注册资本
         */
        double regCaption = 0;
        if (!StringUtils.isEmpty(regCaption1) && !StringUtils.isEmpty(regCaption2)) {

            Double dreg1 = Double.parseDouble(regCaption1);
            Integer ireg1 = dreg1.intValue();
            int l1 = ireg1.toString().length();

            Double dreg2 = Double.parseDouble(regCaption2);
            Integer ireg2 = dreg2.intValue();
            int l2 = ireg2.toString().length();

            int result = Math.abs(l1 - l2);
            if (result == 0) {
                regCaption = 1;
            } else {
                regCaption = 1.0 / result;
            }

        }

        double isHighTech = 0;
        if (isHighTech1 != null && isHighTech1.equals(isHighTech2)) {
            isHighTech = 1;
        }
        double isTop500 = 0;
        if (isTop5001 != null && isTop5001.equals(isTop5002)) {
            isTop500 = 1;
        }
        double isUnicorn = 0;
        if (isUnicorn1 != null && isUnicorn1.equals(isUnicorn2)) {
            isUnicorn = 1;
        }
        double isGazelle = 0;
        if (isGazelle1 != null && isGazelle1.equals(isGazelle2)) {
            isGazelle = 1;
        }
        double isStock = 0;
        if (isStock1 != null && isStock1.equals(isStock2)) {
            isStock = 1;
        }
        double isTaxARate = 0;
        if (isTaxARate1 != null && isTaxARate1.equals(isTaxARate2)) {
            isTaxARate = 1;
        }
        double isAbnormally = 0;
        if (isAbnormally1 != null && isAbnormally1.equals(isAbnormally2)) {
            isAbnormally = 1;
        }
        double noCert = 0;
        if (noCert1 != null && noCert1.equals(noCert2)) {
            noCert = 1;
        }
        double entTypeCN = 0;
        if (entTypeCN1 != null && entTypeCN1.equals(entTypeCN2)) {
            entTypeCN = 1;
        }

        //根据权重，计算各个维度的总分
        double finalSimilar = 50 * scope +
                10 * products +
                4 * regCaption +
                4 * isHighTech +
                4 * isTop500 +
                4 * isUnicorn +
                4 * isGazelle +
                4 * isStock +
                4 * isTaxARate +
                4 * employees +
                4 * isAbnormally +
                4 * noCert +
                4 * entTypeCN;
        return finalSimilar;
    }

    public static void main(String args[]) {
        String s1 = "信息系统服务(依法须经批准的项目，经相关部门批准后方可开展经营活动)";
        String s2 = "正餐服务（依法须经批准的项目，经相关部门批准后方可开展经营活动）";
        System.out.println(Cosine.getSimilarity(s1, s2));
    }
}
