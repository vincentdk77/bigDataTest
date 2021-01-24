package com.atguigu.kemai.es;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.kemai.utils.Constant;
import com.atguigu.kemai.utils.TableName;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;

public class EsHandle {
    public static Integer YES = 0;
    public static Integer No = 1;

    /**
     * 同一个企业entId，针对每一个表的数据进行单独的部分字段处理和清洗
     * @param mangoJson
     * @return
     */
    public static JSONObject transforToEs(JSONObject mangoJson) {
        if (mangoJson == null) return null;
        JSONObject esJson = new JSONObject();

        for (String key : mangoJson.keySet()) {
            // 企业
            if (TableName.MANGO_ENT.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJsonObj = currentArr.getJSONObject(i);
                    JSONObject newJsonObj = new JSONObject();

                    for (String field : currentJsonObj.keySet()) {
                        if (StringUtils.isNotEmpty(currentJsonObj.getString(field))) {
                            if (field.equals("salaryAvg")) {
                                try {
                                    BigDecimal bigDecimal = new BigDecimal(currentJsonObj.getString(field));
                                    double salaryAvg = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                                    newJsonObj.put(field, salaryAvg);
                                } catch (Exception e) {
                                }
                            } else if (field.equals("location")) {
//                                "location":{
//                                    "type":"Point",
//                                    "coordinates":[
//                                        112.862525,
//                                        35.490524
//                                    ]
//                                }
                                try {
                                    JSONObject location = JSONObject.parseObject(currentJsonObj.getString("location"));
                                    JSONArray coordinates = location.getJSONArray("coordinates");
                                    Double[] d = new Double[2];
                                    d[0] = Double.parseDouble(String.valueOf(coordinates.get(0)));
                                    d[1] = Double.parseDouble(String.valueOf(coordinates.get(1)));
                                    newJsonObj.put(field, d);
                                } catch (NumberFormatException e) {
                                }
                            } else {
                                newJsonObj.put(field, currentJsonObj.getString(field));
                            }
                        }
                    }
                    newArray.add(newJsonObj);
//                        String[] fields = {
//                                "uniscId", "entName", "corpStatus", "corpStatusString", "entType", "entTypeString", "entTypeCN", "estDate",
//                                "apprDate", "regState", "regStateCN", "regOrgCn", "regOrg", "regCapCurCN", "regCaption", "regCap", "regNo",
//                                "taxNo", "groupNo", "nodeNum", "legalName", "opFrom", "opTo", "opScope", "dom", "industryPhy", "industryCo",
//                                "eGrpShform", "eGrpName", "ePubGroup", "entTypeForAnnualReport", "entTypeForQuery", "linkmanName", "linkmanCerNo",
//                                "linkmanPhone", "preEntType", "employeesMin", "employeesMax", "regCity", "regProv", "regDistrict",
//                                "annualTurnoverMin", "annualTurnoverMax", "compForm", "compFormCN",
//                                "busExceptCount", "illCount", "notFound", "historyName", "pripid", "products", "desc", "category",
//                                "location", "domain", "entId", "createdAt", "updatedAt", "checkedAt", "createTime",
//                                "updateTime", "licAnth",
//                                "branchCount", "licCount", "insuredCount", "contactCount", "postCount", "recruitCount",
//                                "iCPCount", "lTMPostCount", "lTMJobCount", "certCount", "weiboOrgCount", "lMNewsCount", "prmtKeyCount",
//                                "prmtCompCount", "itemCount", "goodsCount", "appCount", "brandCount", "abnormalCount", "punishmentCount",
//                                "tmValidCount", "tmInvalidCount", "patentCount", "lYPatentCount", "lYPatentPublicCount", "srCount",
//                                "crCount", "wechatCount", "financingCount", "appDownloadCount", "certThisYearCount", "prmtLinksCount",
//                                "ecShopCount",
//                                "domainAgeCount", "appScoreAvg", "salaryAvg", "postUpdateTimeAvg", "errorRateAvg", "respTimeAvg", "isHighTech",
//                                "isTop500", "isUnicorn", "isGazelle", "isStock", "isTaxARate", "isAbnormally", "noLicence", "noInvest",
//                                "noAnnualInvest", "noLinkedin", "noAnnualReport", "noWeibo", "noFiscalAgent", "noMobile", "noTel", "noEmail",
//                                "noDomain", "noICP", "noOnlineService", "noHttps", "noBidding", "noTradeCredit", "noCert", "noWeMedia",
//                                "noWeiboOrg", "noNews", "noWechat", "noPrmt", "noGoods", "noApp", "noBrand", "noPunishment", "noEquityPledge",
//                                "noIllegal", "noMortgage", "noIPRPledge", "noJudicialAid", "noJudicialPaper", "noCourtNotice", "noTrademark",
//                                "noPatent", "noSr", "noCr", "noAbnomalyOpt", "courtNoticeType",
//                                "certType", "stag", "industryCategory", "recruitTitleLM", "systemTags", "agentType",
//                                "postalAddr", "detailAddr", "licNameCN", "brandName", "goodsName"
//                        };
                }
                esJson.put(TableName.MANGO_ENT, newArray);
            }

            // A级纳税人
            if (TableName.MANGO_ENT_A_TAXPAYER.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    // 增量数据根据该字段
                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("taxAYear")) {
                        newJson.put("taxAYear", currentJson.getString("year"));
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_A_TAXPAYER, newArray);
            }

            // 经营异常
            if (TableName.MANGO_ENT_ABNORMAL_OPT.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("speCauseCN")) {
                        newJson.put("speCauseCN", currentJson.getString("speCauseCN"));
                    }
                    if (StringUtils.isNotEmpty("abntime")) {
                        newJson.put("abntime", currentJson.getString("abntime"));
                    }
                    if (StringUtils.isNotEmpty("remDate")) {
                        newJson.put("remDate", currentJson.getString("remDate"));
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_ABNORMAL_OPT, newArray);
            }

            // 应用
            if (TableName.MANGO_ENT_APPS.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("appName")) {
                        newJson.put("appNames", currentJson.getString("appName"));
                    }
                    if (StringUtils.isNotEmpty("appDesc")) {
                        newJson.put("summary", currentJson.getString("appDesc"));   // 高筛
                    }
                    if (StringUtils.isNotEmpty("appCategory")) {
                        newJson.put("appCategory", currentJson.getString("appCategory"));   // 高筛
                    }
                    if (StringUtils.isNotEmpty("appStores")) {
                        newJson.put("appStores", currentJson.getString("appStores"));
                    }
                    if (StringUtils.isNotEmpty("appUpgradeTime")) {
                        newJson.put("appUpgradeTime", currentJson.getString("appUpgradeTime"));
                    }
                    if (StringUtils.isNotEmpty(currentJson.getString("appFraction"))) {
                        newJson.put("appFraction", Double.parseDouble(currentJson.getString("appFraction")));     // 增量统计时计算
                    }
                    if (StringUtils.isNotEmpty(currentJson.getString("appDownloadCount"))) {
                        newJson.put("appDownloadCount", Integer.parseInt(currentJson.getString("appDownloadCount")));   // 增量统计时计算
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_APPS, newArray);
            }

            // 招标
            if (TableName.MANGO_ENT_BIDS.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("entName")) {
                        newJson.put("entName", currentJson.getString("entName"));
                    }
                    if (StringUtils.isNotEmpty("regProv")) {
                        newJson.put("regProv", currentJson.getString("regProv"));
                    }
                    if (StringUtils.isNotEmpty("regCity")) {
                        newJson.put("regCity", currentJson.getString("regCity"));
                    }
                    if (StringUtils.isNotEmpty("regDistrict")) {
                        newJson.put("regDistrict", currentJson.getString("regDistrict"));
                    }
                    if (StringUtils.isNotEmpty("pName")) {
                        newJson.put("pName", currentJson.getString("pName"));
                    }
                    if (StringUtils.isNotEmpty("pubTime")) {
                        newJson.put("pubTime", currentJson.getString("pubTime"));
                    }
                    if (StringUtils.isNotEmpty("agentName")) {
                        newJson.put("agentName", currentJson.getString("agentName"));
                    }
                    if (StringUtils.isNotEmpty("category")) {
                        newJson.put("category", currentJson.getString("category"));
                    }
                    if (StringUtils.isNotEmpty("sourceUrl")) {
                        newJson.put("sourceUrl", currentJson.getString("sourceUrl"));
                    }
                    String category = currentJson.getString("category");
                    if (StringUtils.isNotEmpty(category)) {
                        if (category.equals("工程建设") || category.equals("政府采购") || category.equals("土地") || category.equals("药品采购") || category.equals("国有产权") || category.equals("矿业")) {
                            newJson.put("bidsCategory", category);
                        } else {
                            newJson.put("bidsCategory", "其他");
                        }
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_BIDS, newArray);
            }

            // 品牌
            if (TableName.MANGO_ENT_BRAND.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("name")) {
                        newJson.put("brandName", currentJson.getString("name"));
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_BRAND, newArray);
            }

            // 资质证书
            if (TableName.MANGO_ENT_CERT.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("certType")) {
                        newJson.put("certType", currentJson.getString("certType"));
                    }
                    if (StringUtils.isNotEmpty("certNo")) {
                        newJson.put("certNo", currentJson.getString("certNo"));
                    }
                    if (StringUtils.isNotEmpty("pubDate")) {
                        newJson.put("pubDate", currentJson.getString("pubDate"));
                    }
                    if (StringUtils.isNotEmpty("expireDate")) {
                        newJson.put("expireDate", currentJson.getString("expireDate"));
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_CERT, newArray);
            }

            /** 联系人
             *
             * MOBILE(1, "mobile","手机"),
             * TEL(2, "telephone","固话"),
             * EMAIL(3, "email","邮箱"),
             * QQ(4, "qq","QQ"),
             * WECHAT(5, "weixin","微信"),
             * FAX(6, "fax","传真");
             *
             * mobileSrc telSrc emailSrc qqSrc wxSrc faxSrc
             *
             * 同一联系方式下多个source用“#”拼接，1、2级用“-”拼接
             */
            if (TableName.MANGO_ENT_CONTACTS.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("content")) {
                        newJson.put("content", currentJson.getString("content"));
                    }

                    JSONArray sourceArr = currentJson.getJSONArray("source");
                    StringBuilder contactSource = new StringBuilder();

                    if (sourceArr != null) {
                        for (int j = 0; j < sourceArr.size(); j++) {
                            String name = sourceArr.getJSONObject(j).getString("name");
                            if (StringUtils.isNotEmpty(name)) {
                                if (Constant.B2B.toString().contains(name)) {
                                    contactSource.append("B2B-").append(name).append("#");
                                } else if (Constant.O2O.toString().contains(name)) {
                                    contactSource.append("O2O平台-").append(name).append("#");
                                } else if (Constant.czmh.toString().contains(name)) {
                                    contactSource.append("垂直门户-").append(name).append("#");
                                } else if (Constant.map.toString().contains(name)) {
                                    contactSource.append("地图-").append(name).append("#");
                                } else if (Constant.nianbao.toString().contains(name)) {
                                    if (name.equals("年报")) {
                                        contactSource.append("年报").append("#");
                                    } else {
                                        contactSource.append("年报-").append(name).append("#");
                                    }
                                } else if (Constant.qiye.toString().contains(name)) {
                                    contactSource.append("企业信息-").append(name).append("#");
                                } else if (Constant.shfw.toString().contains(name)) {
                                    contactSource.append("生活服务-").append(name).append("#");
                                } else if (Constant.zhaobiao.toString().contains(name)) {
                                    contactSource.append("招标信息-").append(name).append("#");
                                } else if (Constant.zhaopin.toString().contains(name)) {
                                    contactSource.append("招聘-").append(name).append("#");
                                } else if (Constant.ziliao.toString().contains(name)) {
                                    contactSource.append("资料平台-").append(name).append("#");
                                } else if (Constant.zixun.toString().contains(name)) {
                                    contactSource.append("资讯-").append(name).append("#");
                                } else if (Constant.qita.toString().contains(name)) {
                                    contactSource.append("其他-").append(name).append("#");
                                }
                            }
                        }
                    }

                    String type = currentJson.getString("type");

                    if (StringUtils.isNotEmpty(type) && type.equals("1")) {
                        if (StringUtils.isNotEmpty(contactSource)) {
                            newJson.put("mobileSrc", contactSource.substring(0, contactSource.length() - 1));
                        }
                        newJson.put("entName", "手机" + i);
                        newJson.put("type", type);
                    } else if (StringUtils.isNotEmpty(type) && type.equals("2")) {
                        if (StringUtils.isNotEmpty(contactSource)) {
                            newJson.put("telSrc", contactSource.substring(0, contactSource.length() - 1));
                        }
                        newJson.put("entName", "固话" + i);
                        newJson.put("type", type);
                    } else if (StringUtils.isNotEmpty(type) && type.equals("3")) {
                        if (StringUtils.isNotEmpty(contactSource)) {
                            newJson.put("emailSrc", contactSource.substring(0, contactSource.length() - 1));
                        }
                        newJson.put("entName", "邮箱" + i);
                        newJson.put("type", type);
                    } else if (StringUtils.isNotEmpty(type) && type.equals("4")) {
                        if (StringUtils.isNotEmpty(contactSource)) {
                            newJson.put("wxSrc", contactSource.substring(0, contactSource.length() - 1));
                        }
                        newJson.put("entName", "微信" + i);
                        newJson.put("type", type);
                    } else if (StringUtils.isNotEmpty(type) && type.equals("5")) {
                        if (StringUtils.isNotEmpty(contactSource)) {
                            newJson.put("qqSrc", contactSource.substring(0, contactSource.length() - 1));
                        }
                        newJson.put("entName", "QQ" + i);
                        newJson.put("type", type);
                    } else if (StringUtils.isNotEmpty(type) && type.equals("6")) {
                        if (StringUtils.isNotEmpty(contactSource)) {
                            newJson.put("faxSrc", contactSource.substring(0, contactSource.length() - 1));
                        }
                        newJson.put("entName", "传真" + i);
                        newJson.put("type", type);
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_CONTACTS, newArray);
            }

            // 版权
            if (TableName.MANGO_ENT_COPYRIGHTS.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("name")) {
                        newJson.put("crName", currentJson.getString("name"));
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_CERT, newArray);
            }

            // 法律公告
            if (TableName.MANGO_ENT_COURT_NOTICE.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("caseDesc")) {
                        newJson.put("caseDesc", currentJson.getString("caseDesc").replace("\\r\\n", "").replace(" ", ""));
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_COURT_NOTICE, newArray);
            }

            // 电商店铺
            if (TableName.MANGO_ENT_ECOMMERCE.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("entName")) {
                        newJson.put("entName", currentJson.getString("entName"));
                    }
                    if (StringUtils.isNotEmpty("shopName")) {
                        newJson.put("shopName", currentJson.getString("shopName"));
                    }
                    if (StringUtils.isNotEmpty("shopRate")) {
                        newJson.put("shopRate", currentJson.getString("shopRate"));
                    }
                    if (StringUtils.isNotEmpty("itemCategories")) {
                        newJson.put("itemCategories", currentJson.getString("itemCategories"));
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_ECOMMERCE, newArray);
            }

            // 融资轮次
            if (TableName.MANGO_ENT_FUNDING_EVENT.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("stag")) {
                        newJson.put("stag", currentJson.getString("stag"));
                    }
                    if (StringUtils.isNotEmpty("pubDate")) {
                        newJson.put("pubDate", currentJson.getString("pubDate"));
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_FUNDING_EVENT, newArray);
            }

            // 商品
            if (TableName.MANGO_ENT_GOODS.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("goodsName")) {
                        newJson.put("goodsName", currentJson.getString("goodsName"));
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_GOODS, newArray);
            }

            // 行政许可
            if (TableName.MANGO_ENT_LICENCE.equals(key)) {

                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("licAnth")) {
                        newJson.put("licAnth", currentJson.getString("licAnth"));
                    }
                    if (StringUtils.isNotEmpty("licNameCN")) {
                        newJson.put("licNameCN", currentJson.getString("licNameCN"));
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_LICENCE, newArray);
            }

            // 自媒体
            if (TableName.MANGO_ENT_NEW_MEDIA.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("name")) {
                        newJson.put("mediaTitle", currentJson.getString("name"));
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_NEW_MEDIA, newArray);
            }

            // 新闻
            if (TableName.MANGO_ENT_NEWS.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("title")) {
                        newJson.put("newsTitles", currentJson.getString("title"));
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_NEWS, newArray);
            }

            // 专利
            if (TableName.MANGO_ENT_PATENT.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("name")) {
                        newJson.put("patentName", currentJson.getString("name"));
                    }
                    newJson.put("patentName", currentJson.getString("name"));
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_PATENT, newArray);
            }

            // 行政处罚
            if (TableName.MANGO_ENT_PUNISHMENT.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("issueDate")) {
                        newJson.put("issueDate", currentJson.getString("issueDate"));
                    }
                    if (StringUtils.isNotEmpty("illegalType")) {
                        newJson.put("illegalType", currentJson.getString("illegalType"));
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_PUNISHMENT, newArray);
            }

            // 招聘
            if (TableName.MANGO_ENT_RECRUIT.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("title")) {
                        newJson.put("recruitTitle", currentJson.getString("title"));
                    }
                    if (StringUtils.isNotEmpty("jobDesc")) {
                        newJson.put("jobDesc", currentJson.getString("jobDesc").replace("\n,", "").replace("\n", "").replace(" ", ""));
                    }
                    if (StringUtils.isNotEmpty("salary")) {
                        newJson.put("salary", currentJson.getString("salary"));
                    }
                    if (StringUtils.isNotEmpty("address")) {
                        newJson.put("address", currentJson.getString("address"));
                    }
                    if (StringUtils.isNotEmpty("city")) {
                        newJson.put("city", currentJson.getString("city"));
                    }
                    if (StringUtils.isNotEmpty("prov")) {
                        newJson.put("province", currentJson.getString("prov"));
                    }
                    if (StringUtils.isNotEmpty("salaryFrom")) {
                        newJson.put("salaryFrom", currentJson.getString("salaryFrom"));
                    }
                    if (StringUtils.isNotEmpty("salaryTo")) {
                        newJson.put("salaryTo", currentJson.getString("salaryTo"));
                    }
                    if (StringUtils.isNotEmpty("category")) {
                        newJson.put("postType", currentJson.getString("category"));
                    }
                    if (StringUtils.isNotEmpty("welfare")) {
                        newJson.put("welfare", currentJson.getString("welfare"));
                    }
                    if (StringUtils.isNotEmpty("rdate")) {
                        newJson.put("rdate", currentJson.getString("rdate"));
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_RECRUIT, newArray);
            }

            // 软著
            if (TableName.MANGO_ENT_SOFTWARE.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("name")) {
                        newJson.put("appNames", currentJson.getString("name"));
                    }
                    if (StringUtils.isNotEmpty("pubDate")) {
                        newJson.put("appUpgradeTime", currentJson.getString("pubDate"));
                    }
                    if (StringUtils.isNotEmpty("summary")) {
                        newJson.put("summary", currentJson.getString("summary"));
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_SOFTWARE, newArray);
            }

            // 商标
            if (TableName.MANGO_ENT_TRADEMARK.equals(key)) {

                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("name")) {
                        newJson.put("tmName", currentJson.getString("name"));
                    }
                    if (StringUtils.isNotEmpty("propertyEndDate")) {
                        newJson.put("propertyEndDate", currentJson.getString("propertyEndDate"));
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_TRADEMARK, newArray);
            }

            // 网站
            if (TableName.MANGO_ENT_WEBSITE.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                JSONArray newArray = new JSONArray();

                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);
                    JSONObject newJson = new JSONObject();

                    newJson.put("mongoId", JSONObject.parseObject(currentJson.getString("_id")).getString("$oid"));
                    newJson.put("entId", currentJson.getString("entId"));
                    if (StringUtils.isNotEmpty("domainTitle")) {
                        newJson.put("domainTitle", currentJson.getString("domainTitle"));
                    }
                    if (StringUtils.isNotEmpty("ip")) {
                        newJson.put("ipAddress", currentJson.getString("ip"));
                    }
                    if (StringUtils.isNotEmpty("soWeight")) {
                        newJson.put("soWeight", currentJson.getString("soWeight"));
                    }
                    if (StringUtils.isNotEmpty("baiduIndex")) {
                        newJson.put("baiduIndex", currentJson.getString("baiduIndex"));
                    }
                    if (StringUtils.isNotEmpty("soIndex")) {
                        newJson.put("soIndex", currentJson.getString("soIndex"));
                    }
                    if (StringUtils.isNotEmpty("domainDesc")) {
                        newJson.put("domainDesc", currentJson.getString("domainDesc"));
                    }
                    if (StringUtils.isNotEmpty("domainKeyword")) {
                        newJson.put("domainKeyword", currentJson.getString("domainKeyword"));
                    }
                    //首页布局
                    String indexLayout = "";
                    if (StringUtils.isNotEmpty(currentJson.getString("mUser")) && "1".equals(currentJson.getString("mUser"))) {
                        indexLayout = indexLayout + "注册登录" + ",";
                    }
                    if (StringUtils.isNotEmpty(currentJson.getString("mEnglish")) && "1".equals(currentJson.getString("mEnglish"))) {
                        indexLayout = indexLayout + "英文版本" + ",";
                    }
                    if (StringUtils.isNotEmpty(currentJson.getString("m400")) && "1".equals(currentJson.getString("m400"))) {
                        indexLayout = indexLayout + "400电话" + ",";
                    }
                    if (StringUtils.isNotEmpty(currentJson.getString("mCase")) && "1".equals(currentJson.getString("mCase"))) {
                        indexLayout = indexLayout + "案例" + ",";
                    }
                    if (StringUtils.isNotEmpty(currentJson.getString("mApps")) && "1".equals(currentJson.getString("mApps"))) {
                        indexLayout = indexLayout + "app下载" + ",";
                    }
                    if (StringUtils.isNotEmpty(currentJson.getString("mJionus")) && "1".equals(currentJson.getString("mJionus"))) {
                        indexLayout = indexLayout + "加入我们" + ",";
                    }
                    if (StringUtils.isNotEmpty(currentJson.getString("mProducts")) && "1".equals(currentJson.getString("mProducts"))) {
                        indexLayout = indexLayout + "产品介绍" + ",";
                    }
                    if (StringUtils.isNotEmpty(currentJson.getString("mProducts")) && "1".equals(currentJson.getString("mProducts"))) {
                        indexLayout = indexLayout + "产品介绍" + ",";
                    }
                    if (StringUtils.isNotEmpty(currentJson.getString("mMerchants")) && "1".equals(currentJson.getString("mMerchants"))) {
                        indexLayout = indexLayout + "招商信息" + ",";
                    }
                    if (StringUtils.isNotEmpty(currentJson.getString("mSolution")) && "1".equals(currentJson.getString("mSolution"))) {
                        indexLayout = indexLayout + "解决方案" + ",";
                    }
                    if (StringUtils.isNotEmpty(indexLayout)) {
                        newJson.put("indexLayout", indexLayout);
                    }
                    newArray.add(newJson);
                }
                esJson.put(TableName.MANGO_ENT_WEBSITE, newArray);
            }
        }
        return esJson;
    }

    public static void upsertEnt(JSONObject mangoJson) {
        if (mangoJson == null) return;

        for (String key : mangoJson.keySet()) {
            // 企业
            if (TableName.MANGO_ENT.equals(key)) {  // TODO: 2021/1/24 为啥只往ent表更新数据？
                try {
                    JSONObject ent = mangoJson.getJSONArray(key).getJSONObject(0);
                    JSONObject queryJson = new JSONObject();
                    queryJson.put("entId", ent.getString("entId"));
                    JSONObject existJson = ElasticSearchUtil.search("prod_" + TableName.MANGO_ENT, queryJson);

                    if (existJson == null || StringUtils.isEmpty(existJson.getString("_id"))) {
                        // 插入
                        ElasticSearchUtil.post("prod_" + TableName.MANGO_ENT, ent.toJSONString());
                    } else {
                        // 更新
                        // 以下字段全部取原值或根据本表字段计算，不存其它表关联逻辑，增量时，直接更新
                        String[] sourcesFields = {
                                "uniscId", "entName", "corpStatus", "corpStatusString", "entType", "entTypeString", "entTypeCN", "estDate", "apprDate", "regState",
                                "regStateCN", "regOrgCn", "regOrg", "regCapCurCN", "regCaption", "regCap", "regNo", "taxNo", "groupNo", "nodeNum", "legalName",
                                "opFrom", "opTo", "opScope", "dom", "industryPhy", "industryCo", "eGrpShform", "eGrpName", "ePubGroup", "entTypeForAnnualReport", "entTypeForQuery",
                                "linkmanName", "linkmanCerNo", "linkmanPhone", "preEntType", "employeesMin", "employeesMax", "regCity", "regProv", "regDistrict",
                                "annualTurnoverMin", "annualTurnoverMax", "compForm", "compFormCN", "busExceptCount", "illCount", "notFound", "historyName", "pripid",
                                "products", "desc", "category", "location", "domain", "entId", "createdAt", "updatedAt", "checkedAt", "industryCategory", "agentType"
                        };
                        for (String field : sourcesFields) {
                            existJson.put(field, ent.getString(field));
                        }
                        // 通过文档id来更新
                        ElasticSearchUtil.update("prod_" + TableName.MANGO_ENT, existJson.getString("_id"), existJson);
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT：" + mangoJson.getJSONArray(TableName.MANGO_ENT).toJSONString());
                }
            }
        }
    }

    public static void upsertSubEnt(JSONObject mangoJson) {
        if (mangoJson == null) return;

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();

        for (String key : mangoJson.keySet()) {
            /**
             * A级纳税人：
             * isTaxARate：是否A级纳税人，存在即为1，否则不赋值   √
             */
            if (TableName.MANGO_ENT_A_TAXPAYER.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        // entId 必须在 ent 表中存在
                        if (entJson != null) {
                            // 根据存入的mongo主键 查询是否存在，不存在则新增，存在则更新
                            JSONObject query2 = new JSONObject();
                            String mongoId = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", mongoId);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_A_TAXPAYER, query2);

                            if (queryJson != null) {
                                String[] fields = {"taxAYear"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 根据查询结果获取文档主键_id，更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_A_TAXPAYER, queryJson.getString("_id"), queryJson);
                            } else {
                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_A_TAXPAYER, currentJson.toJSONString());

                                // 更新ENT表的统计字段
                                if (!entJson.getString("isTaxARate").equals("1")) {
                                    entJson.put("isTaxARate", "1");
                                    ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_A_TAXPAYER：" + mangoJson.getJSONArray(TableName.MANGO_ENT_A_TAXPAYER).toJSONString());
                }
            }

            /**
             * 经营异常：
             * isAbnormally：当前是否被列入经营异常，remDate不为空，符合即为0    √
             * noAbnomalyOpt：有无经营异常
             * abnormalCount：经营异常数量，存在记录数   √
             */
            if (TableName.MANGO_ENT_ABNORMAL_OPT.equals(key)) {
                JSONArray currentArr = mangoJson.getJSONArray(key);
                for (int i = 0; i < currentArr.size(); i++) {
                    try {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String mongoId = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", mongoId);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_ABNORMAL_OPT, query2);

                            if (queryJson != null) {
                                // 更新当前表
                                String[] fields = {"speCauseCN", "abntime", "remDate"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT_ABNORMAL_OPT, queryJson.getString("_id"), queryJson);
                            } else {
                                // 更新ENT表的统计字段
                                if (StringUtils.isNotEmpty(currentJson.getString("abntime")) && StringUtils.isEmpty(currentJson.getString("remDate"))) {
                                    if (entJson.getString("isAbnormally").equals("0")) {
                                        entJson.put("isAbnormally", "1");
                                        entJson.put("noAbnomalyOpt", "0");
                                    }
                                    if (StringUtils.isEmpty(entJson.getString("abnormalCount"))) {
                                        entJson.put("abnormalCount", "1");
                                    } else {
                                        int count = Integer.parseInt(entJson.getString("abnormalCount")) + 1;
                                        entJson.put("abnormalCount", count + "");
                                    }
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);

                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_ABNORMAL_OPT, currentJson.toJSONString());
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("--------------------------------------------------");
                        System.out.println("ES更新数据报错：" + e);
                        System.out.println("MANGO_ENT_ABNORMAL_OPT：" + mangoJson.getJSONArray(TableName.MANGO_ENT_ABNORMAL_OPT).toJSONString());
                    }
                }
            }

            /**
             * 年报：
             * noAnnualReport：有无企业年报，存在即为0，否则不赋值    √
             */
            if (TableName.MANGO_ENT_ANNUAL_REPORT.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String mongoId = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", mongoId);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_ANNUAL_REPORT, query2);

                            if (queryJson != null) {
                                // 更新ENT表的统计字段
                                if (entJson.getString("noAnnualReport").equals("1")) {
                                    entJson.put("noAnnualReport", "0");
                                    ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_ANNUAL_REPORT：" + mangoJson.getJSONArray(TableName.MANGO_ENT_ANNUAL_REPORT).toJSONString());
                }
            }

            /**
             * 应用：
             * noApp： 有无应用：存在即为0，否则不赋值  √
             * appCount：应用数量：存在记录数          √
             * appScoreAvg：应用平均分：appScore不为空，计算每个appScore分数，再取平均值  √
             * appDownloadCount：应用总下载量：appDownloadCount，统计所有记录的总数   √
             */
            if (TableName.MANGO_ENT_APPS.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String mongoId = JSONObject.parseObject(currentJson.getString("_id")).getString("$oid");
                            query2.put("mongoId", mongoId);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_APPS, query2);

                            if (queryJson != null) {
                                // 更新ENT表的统计字段
                                // appScoreAvg
                                if (StringUtils.isNotEmpty(currentJson.getString("appFraction"))) {
                                    // 计算差值
                                    double diffScore = Double.parseDouble(currentJson.getString("appFraction")) - Double.parseDouble(queryJson.getString("appFraction"));
                                    double avg = Double.parseDouble(entJson.getString("appScoreAvg"));
                                    int count = Integer.parseInt(entJson.getString("appCount"));
                                    entJson.put("appScoreAvg", (avg + diffScore / count) + "");
                                }
                                // appDownloadCount
                                if (StringUtils.isNotEmpty(currentJson.getString("appDownloadCount"))) {
                                    // 计算差值
                                    int diffCount = Integer.parseInt(currentJson.getString("appDownloadCount")) - Integer.parseInt(queryJson.getString("appDownloadCount"));
                                    int downloadCount = Integer.parseInt(entJson.getString("appDownloadCount"));
                                    entJson.put("appDownloadCount", downloadCount + diffCount);
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);

                                // 更新当前表
                                String[] fields = {"appNames", "summary", "appCategory", "appStores", "appUpgradeTime", "appFraction", "appDownloadCount"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT_APPS, queryJson.getString("_id"), queryJson);
                            } else {
                                // 更新ENT表的统计字段
                                // noApp
                                if (entJson.getString("noApp").equals("1")) {
                                    entJson.put("noApp", "0");
                                }
                                // appCount
                                if (StringUtils.isEmpty(entJson.getString("appCount"))) {
                                    entJson.put("appCount", "1");
                                } else {
                                    int count = Integer.parseInt(entJson.getString("appCount")) + 1;
                                    entJson.put("appCount", count + "");
                                }
                                // appScoreAvg
                                if (StringUtils.isNotEmpty(currentJson.getString("appFraction"))) {
                                    double appScore = Double.parseDouble(currentJson.getString("appFraction"));
                                    double avg = Double.parseDouble(entJson.getString("appScoreAvg"));
                                    int count = Integer.parseInt(entJson.getString("appCount"));
                                    entJson.put("appScoreAvg", (avg * count + appScore) / (count + 1) + "");
                                }
//                              // appDownloadCount
                                if (StringUtils.isNotEmpty(currentJson.getString("appDownloadCount"))) {
                                    int count = Integer.parseInt(currentJson.getString("appDownloadCount"));
                                    int downloadCount = Integer.parseInt(entJson.getString("appDownloadCount"));
                                    entJson.put("appDownloadCount", downloadCount + count);
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);

                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_APPS, currentJson.toJSONString());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_APPS：" + mangoJson.getJSONArray(TableName.MANGO_ENT_APPS).toJSONString());
                }
            }

            /**
             * 招标:
             * 索引表，暂无统计字段
             */
            if (TableName.MANGO_ENT_BIDS.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String mongoId = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", mongoId);     // 为了避免和es主键冲突，es存储的为mongoId
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_BIDS, query2);

                            if (queryJson != null) {
                                String[] fields = {"entName","regProv", "regCity", "regDistrict", "pName", "pubTime",  "agentName", "category", "sourceUrl", "bidsCategory"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_BIDS, queryJson.getString("_id"), queryJson);
                            } else {
                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_BIDS, currentJson.toJSONString());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_BIDS：" + mangoJson.getJSONArray(TableName.MANGO_ENT_BIDS).toJSONString());
                }
            }

            /**
             * 品牌：
             * noBrand：有无品牌信息：存在即为0，否则不赋值   √
             * brandCount：品牌数量：存在记录数    √
             */
            if (TableName.MANGO_ENT_BRAND.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_BRAND, query2);

                            if (queryJson != null) {
                                String[] fields = {"brandName"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_BRAND, _id, queryJson);
                            } else {
                                // 更新ENT表的统计字段
                                if (entJson.getString("noBrand").equals("1")) {
                                    entJson.put("noBrand", "0");
                                }
                                if (StringUtils.isEmpty(entJson.getString("brandCount"))) {
                                    entJson.put("brandCount", "1");
                                } else {
                                    int count = Integer.parseInt(entJson.getString("brandCount")) + 1;
                                    entJson.put("brandCount", count + "");
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);

                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_BRAND, currentJson.toJSONString());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_BRAND：" + mangoJson.getJSONArray(TableName.MANGO_ENT_BRAND).toJSONString());
                }
            }

            /**
             * 资质证书：
             * certType：证书类型：certType取值即可       √
             * noCert：有无证书：存在即为0，否则不赋值      √
             * certCount：证书数量：存在记录数             √
             * certTypeLTM：最近3个月内截止证书类别     ×
             * certTypeLSM：最近6个月内截止证书类别     ×
             * certThisYearCount：本年度获证数量：pubDate不为空且小于一年    ×
             */
            if (TableName.MANGO_ENT_CERT.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_CERT, query2);

                            if (queryJson != null) {
                                String[] fields = {"certType", "certNo", "pubDate", "expireDate"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_CERT, _id, queryJson);

                                // 更新ENT表的统计字段
                                // certType为标签字段
                                if (StringUtils.isNotEmpty(currentJson.getString("certType"))) {
                                    if (StringUtils.isEmpty(entJson.getString("certType"))) {
                                        entJson.put("certType", currentJson.getString("certType"));
                                    } else {
                                        String certType = entJson.getString("certType");
                                        String[] words = certType.split(",");
                                        HashSet<String> set = new HashSet<String>();
                                        Collections.addAll(set, words);
                                        set.add(currentJson.getString("certType"));
                                        StringBuilder sb = new StringBuilder();
                                        for (String word : set) {
                                            sb.append(",").append(word);
                                        }
                                        entJson.put("certType", sb.substring(1));
                                    }
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);
                            } else {
                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_CERT, currentJson.toJSONString());
                                // 更新ENT表的统计字段
                                // certType为标签字段
                                if (StringUtils.isNotEmpty(currentJson.getString("certType"))) {
                                    if (StringUtils.isEmpty(entJson.getString("certType"))) {
                                        entJson.put("certType", currentJson.getString("certType"));
                                    } else {
                                        String certType = entJson.getString("certType");
                                        String[] words = certType.split(",");
                                        HashSet<String> set = new HashSet<String>();
                                        Collections.addAll(set, words);
                                        set.add(currentJson.getString("certType"));
                                        StringBuilder sb = new StringBuilder();
                                        for (String word : set) {
                                            sb.append(",").append(word);
                                        }
                                        entJson.put("certType", sb.substring(1));
                                    }
                                }
                                if (entJson.getString("noCert").equals("1")) {
                                    entJson.put("noCert", "0");
                                }
                                if (StringUtils.isEmpty(entJson.getString("certCount"))) {
                                    entJson.put("certCount", "1");
                                } else {
                                    int count = Integer.parseInt(entJson.getString("certCount")) + 1;
                                    entJson.put("certCount", count + "");
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_CERT：" + mangoJson.getJSONArray(TableName.MANGO_ENT_CERT).toJSONString());
                }
            }

            /**
             * 联系方式：
             * noMobile：有无手机：type=1，符合条件即为0，否则不赋值   √
             * noTel：有无固话：type=2，符合条件即为0，否则不赋值      √
             * noEmail：有无邮箱：type=3，符合条件即为0，否则不赋值    √
             * contactCount：联系人数量：type!=6，符合条件记录数       √
             */
            if (TableName.MANGO_ENT_CONTACTS.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_CONTACTS, query2);

                            if (queryJson != null) {
                                String[] fields = {"content"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_CONTACTS, _id, queryJson);
                            } else {
                                // 更新ENT表的统计字段
                                String type = currentJson.getString("type");
                                if (type.equals("1") && entJson.getString("noMobile").equals("1")) {
                                    entJson.put("noMobile", "0");
                                } else if (type.equals("2") && entJson.getString("noTel").equals("1")) {
                                    entJson.put("noTel", "0");
                                } else if (type.equals("3") && entJson.getString("noEmail").equals("1")) {
                                    entJson.put("noEmail", "0");
                                }
                                if (!type.equals("6")) {
                                    if (StringUtils.isEmpty(currentJson.getString("contactCount"))) {
                                        entJson.put("contactCount", 1);
                                    } else {
                                        int contactCount = Integer.parseInt(currentJson.getString("contactCount")) + 1;
                                        entJson.put("contactCount", contactCount + "");
                                    }
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);

                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_CONTACTS, currentJson.toJSONString());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_CONTACTS：" + mangoJson.getJSONArray(TableName.MANGO_ENT_CONTACTS).toJSONString());
                }
            }

            /**
             * 版权:
             * 索引表，暂无统计字段
             */
            if (TableName.MANGO_ENT_COPYRIGHTS.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_COPYRIGHTS, query2);

                            if (queryJson != null) {
                                String[] fields = {"crName"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_COPYRIGHTS, _id, queryJson);
                            } else {
                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_COPYRIGHTS, currentJson.toJSONString());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_COPYRIGHTS：" + mangoJson.getJSONArray(TableName.MANGO_ENT_COPYRIGHTS).toJSONString());
                }
            }

            /**
             * 被执行人：
             * 索引表，暂无统计字段
             */
            if (TableName.MANGO_ENT_COURT_OPERATOR.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_COURT_OPERATOR, query2);

                            if (queryJson != null) {
                                String[] fields = {"caseDesc"};

                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_COURT_OPERATOR, _id, queryJson);
                            } else {
                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_COURT_OPERATOR, currentJson.toJSONString());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_COURT_OPERATOR：" + mangoJson.getJSONArray(TableName.MANGO_ENT_COURT_OPERATOR).toJSONString());
                }
            }

            /**
             * 电商店铺：
             * ecShopCount：电商店铺数量：shopName去重，总记录数   √
             */
            if (TableName.MANGO_ENT_ECOMMERCE.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_ECOMMERCE, query2);

                            if (queryJson != null) {
                                String[] fields = {"entName", "shopName", "shopRate", "itemCategories"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_ECOMMERCE, _id, queryJson);
                            } else {
                                // 更新ENT表的统计字段
                                if (StringUtils.isEmpty(entJson.getString("ecShopCount"))) {
                                    entJson.put("ecShopCount", "1");
                                } else {
                                    int count = Integer.parseInt(entJson.getString("ecShopCount")) + 1;
                                    entJson.put("ecShopCount", count + "");
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);

                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_ECOMMERCE, currentJson.toJSONString());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_ECOMMERCE：" + mangoJson.getJSONArray(TableName.MANGO_ENT_ECOMMERCE).toJSONString());
                }
            }

            /**
             * 股权出资：
             * noEquityPledge：有无股权出资：存在即为0，否则不赋值    √
             */
            if (TableName.MANGO_ENT_EQUITY_PLEDGED.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_EQUITY_PLEDGED, query2);

                            if (queryJson != null) {
                            } else {
                                // 更新ENT表的统计字段
                                if (entJson.getString("noEquityPledge").equals("1")) {
                                    entJson.put("noEquityPledge", "1");
                                } else {
                                    int count = Integer.parseInt(entJson.getString("noEquityPledge")) + 1;
                                    entJson.put("noEquityPledge", count + "");
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_EQUITY_PLEDGED：" + mangoJson.getJSONArray(TableName.MANGO_ENT_EQUITY_PLEDGED).toJSONString());
                }
            }

            /**
             * 融资事件：
             * stag：融资轮次    √
             */
            if (TableName.MANGO_ENT_FUNDING_EVENT.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_FUNDING_EVENT, query2);

                            // 查询esJson中stag取的是哪条记录
                            JSONObject query3 = new JSONObject();
                            query3.put("entId", currentJson.getString("entId"));
                            query3.put("stag", entJson.getString("stag"));
                            JSONObject queryJson2 = ElasticSearchUtil.search(TableName.MANGO_ENT_FUNDING_EVENT, query3);

                            if (queryJson != null) {
                                // 更新ENT表的统计字段
                                if (StringUtils.isNotEmpty(currentJson.getString("stag"))) {
                                    if (StringUtils.isEmpty(entJson.getString("stag"))) {
                                        entJson.put("stag", currentJson.getString("stag"));
                                    } else {
                                        try {
                                            if (format.parse(currentJson.getString("pubDate")).getTime() > format.parse(queryJson2.getString("pubDate")).getTime()) {
                                                entJson.put("stag", currentJson.getString("stag"));
                                            }
                                        } catch (ParseException e) {
                                        }
                                    }
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);

                                // 更新当前表
                                String[] fields = {"stag", "pubDate"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT_FUNDING_EVENT, _id, queryJson);
                            } else {
                                // 更新ENT表的统计字段
                                if (StringUtils.isNotEmpty(currentJson.getString("stag"))) {
                                    if (StringUtils.isEmpty(entJson.getString("stag"))) {
                                        entJson.put("stag", currentJson.getString("stag"));
                                    } else {
                                        try {
                                            if (format.parse(currentJson.getString("pubDate")).getTime() > format.parse(queryJson2.getString("pubDate")).getTime()) {
                                                entJson.put("stag", currentJson.getString("stag"));
                                            }
                                        } catch (ParseException e) {
                                        }
                                    }
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);

                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_FUNDING_EVENT, currentJson.toJSONString());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_FUNDING_EVENT：" + mangoJson.getJSONArray(TableName.MANGO_ENT_FUNDING_EVENT).toJSONString());
                }
            }

            /**
             * 商品：
             * noGoods：有无商品     √
             * goodsCount：商品数量        √
             */
            if (TableName.MANGO_ENT_GOODS.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_GOODS, query2);

                            if (queryJson != null) {
                                String[] fields = {"goodsName"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_GOODS, _id, queryJson);
                            } else {
                                // 更新ENT表的统计字段
                                if (entJson.getString("noGoods").equals("1")) {
                                    entJson.put("noGoods", "0");
                                }
                                if (StringUtils.isEmpty(entJson.getString("goodsCount"))) {
                                    entJson.put("goodsCount", "1");
                                } else {
                                    int count = Integer.parseInt(entJson.getString("goodsCount")) + 1;
                                    entJson.put("goodsCount", count + "");
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);

                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_GOODS, currentJson.toJSONString());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_GOODS：" + mangoJson.getJSONArray(TableName.MANGO_ENT_GOODS).toJSONString());
                }
            }

            /**
             * 成长型企业：
             * isGazelle：是否瞪羚企业：category=1，符合即为1    √
             * isUnicorn：是否独角兽企业：category=2，符合即为1   √
             * isHighTech：是否为高新企业：category=3，符合即为1  √
             */
            if (TableName.MANGO_ENT_GROWINGUP.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_LICENCE, query2);

                            if (queryJson != null) {
                            } else {
                                // 更新ENT表的统计字段
                                String category = currentJson.getString("category");
                                if (category.equals("1") && entJson.getString("isGazelle").equals("0")) {
                                    entJson.put("isGazelle", "1");
                                } else if (category.equals("2") && entJson.getString("isUnicorn").equals("0")) {
                                    entJson.put("isUnicorn", "1");
                                } else if (category.equals("3") && entJson.getString("isHighTech").equals("0")) {
                                    entJson.put("isHighTech", "1");
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_GROWINGUP：" + mangoJson.getJSONArray(TableName.MANGO_ENT_GROWINGUP).toJSONString());
                }
            }

            /**
             * 对外投资：
             * noInvest：有无对外投资：brunch不为空且isBrunch=0，符合即为0           √
             * branchCount：分支机构数：brunch不为空且isBrunch=1，符合条件的记录数      √
             */
            if (TableName.MANGO_ENT_INVESTCOMPANY.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_LICENCE, query2);

                            if (queryJson != null) {
                            } else {
                                // 更新ENT表的统计字段
                                if (StringUtils.isNotEmpty(currentJson.getString("brunch"))) {
                                    if (currentJson.getString("isBrunch").equals("0")) {
                                        if (entJson.getString("noInvest").equals("1")) {
                                            entJson.put("noInvest", "0");
                                        }
                                    } else if (currentJson.getString("isBrunch").equals("1")) {
                                        if (StringUtils.isEmpty(entJson.getString("branchCount"))) {
                                            entJson.put("branchCount", 1);
                                        } else {
                                            int branchCount = Integer.parseInt(entJson.getString("branchCount")) + 1;
                                            entJson.put("branchCount", branchCount + "");
                                        }
                                    }
                                    ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_INVESTCOMPANY：" + mangoJson.getJSONArray(TableName.MANGO_ENT_INVESTCOMPANY).toJSONString());
                }
            }

            /**
             * 行政许可：
             * noLicence：有无行政许可：valTo（有效期）不为空且大于当前日期，符合即为0      √
             * licCount：行政许可数量：存在记录数    √
             */
            if (TableName.MANGO_ENT_LICENCE.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_LICENCE, query2);

                            if (queryJson != null) {
                                String[] fields = {"licNameCN", "licAnth"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_LICENCE, _id, queryJson);
                            } else {
                                // 更新ENT表的统计字段
                                if (entJson.getString("noLicence").equals("1")) {
                                    entJson.put("noLicence", "0");
                                    entJson.put("licCount", "1");
                                } else {
                                    int licCount = Integer.parseInt(entJson.getString("licCount")) + 1;
                                    entJson.put("licCount", licCount + "");
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);

                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_LICENCE, currentJson.toJSONString());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_LICENCE：" + mangoJson.getJSONArray(TableName.MANGO_ENT_LICENCE).toJSONString());
                }
            }

            /**
             * 上市公司：
             * isStock：是否上市企业：存在即为1，否则不赋值       √
             */
            if (TableName.MANGO_ENT_LISTED.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);     // 为了避免和es主键冲突，es存储的为mongoId
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_LISTED, query2);

                            if (queryJson != null) {
                            } else {
                                // 更新ENT表的统计字段
                                if (entJson.getString("isStock").equals("0")) {
                                    entJson.put("isStock", "1");
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_LISTED：" + mangoJson.getJSONArray(TableName.MANGO_ENT_LISTED).toJSONString());
                }
            }

            /**
             * 新媒体：
             * noWeMedia：有无自媒体：存在即为0，否则不赋值      √
             * noWechat：有无微信公众号：type=1，存在即为0，否则不赋值  √
             * noWeibo：有无微博：type=2，存在即为0，否则不赋值  √
             * wechatCount：微信公众号数：type=1，存在记录数      √
             * weiboOrgCount：机构微博数：type=2，存在记录数     √
             */
            if (TableName.MANGO_ENT_NEW_MEDIA.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);     // 为了避免和es主键冲突，es存储的为mongoId
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_NEW_MEDIA, query2);

                            if (queryJson != null) {
                                String[] fields = {"mediaTitle"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_NEW_MEDIA, _id, queryJson);
                            } else {
                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_NEW_MEDIA, currentJson.toJSONString());

                                // 更新ENT表的统计字段
                                if (entJson.getString("noWeMedia").equals("1")) {
                                    entJson.put("noWeMedia", "0");
                                }
                                String type = currentJson.getString("type");
                                if (type.equals("1") && entJson.getString("noWechat").equals("1")) {
                                    entJson.put("noWechat", "0");
                                    entJson.put("wechatCount", "1");
                                } else if (type.equals("2") && entJson.getString("noWeibo").equals("1")) {
                                    entJson.put("noWeibo", "0");
                                    entJson.put("weiboOrgCount", "1");
                                }
                                if (StringUtils.isNotEmpty("wechatCount")) {
                                    int wechatCount = Integer.parseInt(entJson.getString("wechatCount")) + 1;
                                    entJson.put("wechatCount", wechatCount + "");
                                }
                                if (StringUtils.isNotEmpty("weiboOrgCount")) {
                                    int weiboOrgCount = Integer.parseInt(entJson.getString("weiboOrgCount")) + 1;
                                    entJson.put("weiboOrgCount", weiboOrgCount + "");
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_NEW_MEDIA：" + mangoJson.getJSONArray(TableName.MANGO_ENT_NEW_MEDIA).toJSONString());
                }
            }

            /**
             * 新闻：
             * noNews：有无新闻：存在即为0，否则不赋值      √
             * lMNewsCount：近1月新闻数量：pubDate小于1个月，符合条件的记录数    ×
             */
            if (TableName.MANGO_ENT_NEWS.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_NEWS, query2);

                            if (queryJson != null) {
                                String[] fields = {"newsTitles"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_NEWS, _id, queryJson);
                            } else {
                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_NEWS, currentJson.toJSONString());

                                // 更新ENT表的统计字段
                                if (entJson.getString("noNews").equals("1")) {
                                    entJson.put("noNews", "0");
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_NEWS：" + mangoJson.getJSONArray(TableName.MANGO_ENT_NEWS).toJSONString());
                }
            }

            /**
             * 专利：
             * noPatent：有无专利：存在即为0，否则不赋值    √
             * lYPatentCount：最近一年申请专利数量：appDate不为空且小于一年     ×
             * lYPatentPublicCount：最近一年公开专利数量：pubDate不为空且小于一年   ×
             */
            if (TableName.MANGO_ENT_PATENT.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_PATENT, query2);

                            if (queryJson != null) {
                                String[] fields = {"patentName"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_PATENT, _id, queryJson);
                            } else {
                                // 更新ENT表的统计字段
                                if (entJson.getString("noPatent").equals("1")) {
                                    entJson.put("noPatent", "0");
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);

                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_PATENT, currentJson.toJSONString());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_PATENT：" + mangoJson.getJSONArray(TableName.MANGO_ENT_PATENT).toJSONString());
                }
            }

            /**
             * 行政处罚：
             * noPunishment：有无行政处罚：存在即为0，否则不赋值      √
             * punishmentCount：行政处罚数量       √
             */
            if (TableName.MANGO_ENT_PUNISHMENT.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_PUNISHMENT, query2);

                            if (queryJson != null) {
                                String[] fields = {"issueDate", "illegalType"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_PUNISHMENT, _id, queryJson);
                            } else {
                                // 更新ENT表的统计字段
                                if (entJson.getString("noPunishment").equals("1")) {
                                    entJson.put("noPunishment", "0");
                                }
                                if (StringUtils.isEmpty(entJson.getString("punishmentCount"))) {
                                    entJson.put("punishmentCount", "1");
                                } else {
                                    int punishmentCount = Integer.parseInt(entJson.getString("punishmentCount")) + 1;
                                    entJson.put("punishmentCount", punishmentCount + "");
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);

                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_PUNISHMENT, currentJson.toJSONString());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_PUNISHMENT：" + mangoJson.getJSONArray(TableName.MANGO_ENT_PUNISHMENT).toJSONString());
                }
            }

            /**
             * 招聘：
             * salaryAvg：岗位平均薪酬：salaryFrom不为空，salaryTo不为空且不等于"面议"，二者先求平均值，再累加求平均值
             * postUpdateTimeAvg：岗位平均更新频率：rdate（发布日期），取最新的日期
             * recruitTitleLM：最近1个月招聘岗位名称：title，，拼接所有记录的值
             * recruitCount：当前招聘人数：存在记录数
             * postCount：当前招聘岗位数：title+jobDesc后去重，符合条件的记录数
             * lTMJobCount：近三个月招聘总人数：rdate（发布日期）小于3月，符合条件的记录数
             * lTMPostCount：近三个月招聘岗位数：rdate（发布日期）小于3月，title去重，符合条件的记录数
             */
            if (TableName.MANGO_ENT_RECRUIT.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_RECRUIT, query2);

                            if (queryJson != null) {
                                String[] fields = {"recruitTitle", "jobDesc", "salary", "address", "city", "province", "salaryFrom", "salaryTo", "postType", "welfare", "rdate"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_RECRUIT, _id, queryJson);
                            } else {
                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_RECRUIT, currentJson.toJSONString());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_RECRUIT：" + mangoJson.getJSONArray(TableName.MANGO_ENT_RECRUIT).toJSONString());
                }
            }

            /**
             * 软著：
             * noSr：有无软著：存在即为0，否则不赋值    √
             * srCount：软著数量     √
             */
            if (TableName.MANGO_ENT_SOFTWARE.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_SOFTWARE, query2);

                            if (queryJson != null) {
                                String[] fields = {"appNames", "appPublishTime", "summary"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_SOFTWARE, _id, queryJson);
                            } else {
                                // 更新ENT表的统计字段
                                if (entJson.getString("noSr").equals("1")) {
                                    entJson.put("noSr", "0");
                                }
                                if (StringUtils.isEmpty(entJson.getString("srCount"))) {
                                    entJson.put("srCount", "1");
                                } else {
                                    int punishmentCount = Integer.parseInt(entJson.getString("srCount")) + 1;
                                    entJson.put("srCount", punishmentCount + "");
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);

                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_SOFTWARE, currentJson.toJSONString());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_SOFTWARE：" + mangoJson.getJSONArray(TableName.MANGO_ENT_SOFTWARE).toJSONString());
                }
            }

            /**
             * 500强：
             * isTop500：是否500强企业：存在即为1，否则不赋值    √
             */
            if (TableName.MANGO_ENT_TOP500.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_TOP500, query2);

                            if (queryJson != null) {
                            } else {
                                // 更新ENT表的统计字段
                                if (entJson.getString("isTop500").equals("0")) {
                                    entJson.put("isTop500", "1");
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_TOP500：" + mangoJson.getJSONArray(TableName.MANGO_ENT_TOP500).toJSONString());
                }
            }

            /**
             * 商标：
             * noTrademark：有无商标：存在即为0，否则不赋值     √
             */
            if (TableName.MANGO_ENT_TRADEMARK.equals(key)) {
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_TRADEMARK, query2);

                            if (queryJson != null) {
                                String[] fields = {"tmName", "propertyEndDate"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_TRADEMARK, _id, queryJson);
                            } else {
                                // 更新ENT表的统计字段
                                if (entJson.getString("noTrademark").equals("1")) {
                                    entJson.put("noTrademark", "0");
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);

                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_TRADEMARK, currentJson.toJSONString());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_TRADEMARK：" + mangoJson.getJSONArray(TableName.MANGO_ENT_TRADEMARK).toJSONString());
                }
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
                try {
                    JSONArray currentArr = mangoJson.getJSONArray(key);
                    for (int i = 0; i < currentArr.size(); i++) {
                        JSONObject currentJson = currentArr.getJSONObject(i);
                        JSONObject query = new JSONObject();
                        query.put("entId", currentJson.getString("endId"));
                        JSONObject entJson = ElasticSearchUtil.search(TableName.MANGO_ENT, query);

                        if (entJson != null) {
                            JSONObject query2 = new JSONObject();
                            String _id = JSONObject.parseObject(currentArr.getJSONObject(i).getString("_id")).getString("$oid");
                            query2.put("mongoId", _id);
                            JSONObject queryJson = ElasticSearchUtil.search(TableName.MANGO_ENT_WEBSITE, query2);

                            if (queryJson != null) {
                                String[] fields = {"domainTitle", "ipAddress", "soWeight", "baiduIndex", "soIndex", "domainDesc", "domainKeyword", "indexLayout"};
                                for (String field : fields) {
                                    if (StringUtils.isNotEmpty(currentJson.getString(field))) {
                                        queryJson.put(field, currentJson.getString(field));
                                    }
                                }
                                // 更新当前表
                                ElasticSearchUtil.update(TableName.MANGO_ENT_WEBSITE, _id, queryJson);
                            } else {
                                if (currentJson.getJSONArray("icpList")!=null&&currentJson.getJSONArray("icpList").size()>0){
                                    if (entJson.getString("noICP").equals("1")){
                                        entJson.put("noICP", "0");
                                    }
                                }
                                if (StringUtils.isNotEmpty(currentJson.getString("domain"))) {
                                    if (entJson.getString("noDomain").equals("1")) {
                                        entJson.put("noDomain", "0");
                                    }
                                }
                                if (currentJson.getString("mCc").equals("1")) {
                                    if (entJson.getString("noOnlineService").equals("1")) {
                                        entJson.put("noOnlineService", "0");
                                    }
                                }
                                if (currentJson.getString("mHttps").equals("1")) {
                                    if (entJson.getString("noHttps").equals("1")) {
                                        entJson.put("noHttps", "0");
                                    }
                                }
                                ElasticSearchUtil.update(TableName.MANGO_ENT, entJson.getString("_id"), entJson);

                                // 插入当前表
                                ElasticSearchUtil.post(TableName.MANGO_ENT_WEBSITE, currentJson.toJSONString());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("ES更新数据报错：" + e);
                    System.out.println("MANGO_ENT_WEBSITE：" + mangoJson.getJSONArray(TableName.MANGO_ENT_WEBSITE).toJSONString());
                }
            }
        }
    }
}
