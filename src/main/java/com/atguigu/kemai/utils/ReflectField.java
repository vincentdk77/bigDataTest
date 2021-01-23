package com.atguigu.kemai.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class ReflectField {

    public static Map<String, String> classMap;

    static {
        classMap = new HashMap<String, String>();
        classMap.put("cert", "com.kemai.entity.mango.EntCert");
        classMap.put("ent_abnorma_opt", "com.kemai.entity.mango.EntAbnormalOpt");
        classMap.put("ent_alterinfo", "com.kemai.entity.mango.EntAlterinfo");
        classMap.put("ent_annual_report", "com.kemai.entity.mango.EntAnnualReport");
        classMap.put("ent_apps", "com.kemai.entity.mango.EntApps");
        classMap.put("ent_brand", "com.kemai.entity.mango.EntBrand");
        classMap.put("ent_contacts", "com.kemai.entity.mango.EntContacts");
        classMap.put("ent_domain", "com.kemai.entity.mango.EntDomain");
        classMap.put("ent_ecommerce", "com.kemai.entity.mango.EntEcommerce");
        classMap.put("ent_equity_pledged", "com.kemai.entity.mango.EntEquityPledged");
        classMap.put("ent_goods", "com.kemai.entity.mango.EntGoods");
        classMap.put("ent_history", "com.kemai.entity.mango.EntHistory");
        classMap.put("ent_invest_company", "com.kemai.entity.mango.EntInvestCompany");
        classMap.put("ent_item", "com.kemai.entity.mango.EntItem");
        classMap.put("ent_key_person", "com.kemai.entity.mango.EntKeyPerson");
        classMap.put("ent_lawsuit", "com.kemai.entity.mango.EntLawsuit");
        classMap.put("ent_licence", "com.kemai.entity.mango.EntLicence");
        classMap.put("ent_new_media", "com.kemai.entity.mango.EntNewMedia");
        classMap.put("ent_ent_news", "com.kemai.entity.mango.EntNews");

        classMap.put("ent_promotion", "com.kemai.entity.mango.EntPromotion");
        classMap.put("ent_punishment", "com.kemai.entity.mango.EntPunishment");
        classMap.put("ent_recruit", "com.kemai.entity.mango.EntRecruit");
        classMap.put("ent_share_holder", "com.kemai.entity.mango.EntShareholder");
        classMap.put("ent_software", "com.kemai.entity.mango.EntSoftware");
        classMap.put("ent_trademark", "com.kemai.entity.mango.EntTrademark");
        classMap.put("ent_website", "com.kemai.entity.mango.EntWebsite");
        classMap.put("ent_patent", "com.kemai.entity.mango.EntPatent");
    }

    public static void setField(JSONObject json, Object object) {
        try {
            Method[] methods = object.getClass().getMethods();
            for (Method m : methods) {
                String methodName = m.getName();
                if (methodName.startsWith("set")) {
                    String field = methodName.substring(3, 4).toLowerCase() + methodName.substring(4, methodName.length());
                    m.invoke(object, json.getString(field));
                }
            }
        } catch (Exception e) {
        }
    }

    public static void main(String args[]) {
        try {
            Class cls = Class.forName(classMap.get("ent"));
            Object c = cls.newInstance();
            String s = "{\"regCap\": \"壹佰伍拾万元整\", \"regCapCurCN\": \"人民币\", \"nodeNum\": \"440000\", \"preEntType\": \"1130\", \"entName\": \"清远福元农业科技有限公司\", \"regNo\": \"441802000004126\", \"uniscId\": \"91441802351267997C\", \"regState\": \"4\", \"regStateCN\": \"注销企业\", \"entType\": \"1\", \"entTypeCN\": \"有限责任公司(自然人投资或控股)\", \"legalName\": \"李英\", \"industryPhy\": \"M\", \"regOrg\": \"441802\", \"regOrgCn\": \"广东省清远市清城区工商行政管理局\", \"estDate\": \"2015-06-15\", \"apprDate\": \"2019-09-02\", \"dom\": \"清远市清城区人民三路31号清远富荣农副产品批发中心8栋16号\", \"opFrom\": \"2015-06-15\", \"opTo\": null, \"opScope\": \"农业技术推广服务，批发零售业，农产品加工，贸易代理，网站建设，软件开发，种植农作物水果蔬菜。(依法须经批准的项目，经相关部门批准后方可开展经营活动)〓\", \"eGrpName\": null, \"eGrpShform\": null, \"ePubGroup\": \"unshow\", \"entTypeForAnnualReport\": 1, \"entTypeForQuery\": 1, \"linkmanName\": \"\", \"linkmanCerNo\": \"\", \"linkmanPhone\": \"\", \"updatedAt\": 1588476351.85796, \"keyperson\": [{\"perId\": \"d7d14569-016c-1000-e005-f7390a130116441800\", \"pripId\": \"f5a65f99-014d-1000-e000-0b3d0a130116\", \"name\": \"黄海林\", \"positionCN\": \"监事\", \"createdAt\": 1588476402.434772}, {\"perId\": \"d7d14569-016c-1000-e005-f7370a130116441800\", \"pripId\": \"f5a65f99-014d-1000-e000-0b3d0a130116\", \"name\": \"李英\", \"positionCN\": \"执行董事\", \"createdAt\": 1588476402.451218}, {\"perId\": \"d7d14569-016c-1000-e005-f7380a130116441800\", \"pripId\": \"f5a65f99-014d-1000-e000-0b3d0a130116\", \"name\": \"王凡\", \"positionCN\": \"经理\", \"createdAt\": 1588476402.464797}], \"shareholder\": [{\"invId\": \"6E2B48C760368135AD1A951B8334A77DB77D5D7D5D7DD808AD0EC4D9A83C0D2D8EFFCFFE5D7D5D7D4C4C6C4C5160CB1B9D5777208305258625549E54F7052505-1588475872400\", \"inv\": \"王海林\", \"invTypeCN\": \"自然人股东\", \"cerTypeCN\": \"中华人民共和国居民身份证\", \"cerNo\": \"\", \"blicTypeCN\": \"\", \"liSubConAm\": null, \"liAcConAm\": null, \"countryCN\": \"\", \"dom\": \"\", \"respFormCN\": \"\", \"respForm\": \"\", \"sConForm\": \"货币出资\", \"sConFormCN\": null, \"conDate\": null, \"detailCheck\": \"false\", \"url\": \"http://192.168.199.194:8082/gsxt/corp-query-entprise-info-shareholderDetail-6E2B48C760368135AD1A951B8334A77DB77D5D7D5D7DD808AD0EC4D9A83C0D2D8EFFCFFE5D7D5D7D4C4C6C4C5160CB1B9D5777208305258625549E54F7052505-1588475872400.html?entType=1\", \"createdAt\": 1588476405.101828}, {\"invId\": \"175231BE194FF84CD463EC62FA4DDE04CE0424042404A171D477BDA0D1457454F786B68724042404353515352819B262E4624215B63010B31061AB61C2301030-1588475872400\", \"inv\": \"李英\", \"invTypeCN\": \"自然人股东\", \"cerTypeCN\": \"中华人民共和国居民身份证\", \"cerNo\": \"\", \"blicTypeCN\": \"\", \"liSubConAm\": null, \"liAcConAm\": null, \"countryCN\": \"\", \"dom\": \"\", \"respFormCN\": \"\", \"respForm\": \"\", \"sConForm\": \"货币出资\", \"sConFormCN\": null, \"conDate\": null, \"detailCheck\": \"false\", \"url\": \"http://192.168.199.194:8082/gsxt/corp-query-entprise-info-shareholderDetail-175231BE194FF84CD463EC62FA4DDE04CE0424042404A171D477BDA0D1457454F786B68724042404353515352819B262E4624215B63010B31061AB61C2301030-1588475872400.html?entType=1\", \"createdAt\": 1588476405.114481}, {\"invId\": \"F1B4D758FFA91EAA32850A841CAB38E228E2C2E2C2E2479732915B4637A392B211605061C2E2C2E2D3D3F3D3CEFF5484021F3F68CB4D6DCE6D1CD61CBF4D6D4D-1588475872400\", \"inv\": \"王凡\", \"invTypeCN\": \"自然人股东\", \"cerTypeCN\": \"中华人民共和国居民身份证\", \"cerNo\": \"\", \"blicTypeCN\": \"\", \"liSubConAm\": null, \"liAcConAm\": null, \"countryCN\": \"\", \"dom\": \"\", \"respFormCN\": \"\", \"respForm\": \"\", \"sConForm\": \"货币出资\", \"sConFormCN\": null, \"conDate\": null, \"detailCheck\": \"false\", \"url\": \"http://192.168.199.194:8082/gsxt/corp-query-entprise-info-shareholderDetail-F1B4D758FFA91EAA32850A841CAB38E228E2C2E2C2E2479732915B4637A392B211605061C2E2C2E2D3D3F3D3CEFF5484021F3F68CB4D6DCE6D1CD61CBF4D6D4D-1588475872400.html?entType=1\", \"createdAt\": 1588476405.128661}]}";
            JSONObject json = JSON.parseObject(s);
            setField(json, c);
            System.out.println(c);
        } catch (Exception e) {
        }
    }
}
