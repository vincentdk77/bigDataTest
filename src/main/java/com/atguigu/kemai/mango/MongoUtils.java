package com.atguigu.kemai.mango;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.kemai.utils.TableName;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.internal.MongoClientImpl;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.*;


public class MongoUtils {
    private static final MongoTemplate template;

    static {
        //由于mongoDB的一个数据库会有一个密码，换一个数据库就连不上了！
//       String DB = "ent";
//        MongoClient client = new MongoClient(
//                new ServerAddress("node11", 28018),
//                MongoCredential.createCredential("spark", DB, "spark$890$3".toCharArray()),
//                MongoClientOptions.builder().sslEnabled(false).build());
//        template = new MongoTemplate(client, DB);

        String DB = "crm";
        MongoClient client = new MongoClient(
                new ServerAddress("node9", 28018),
                MongoCredential.createCredential("kemai", DB, "keimai@123!".toCharArray()),
                MongoClientOptions.builder().sslEnabled(false).build());
        template = new MongoTemplate(client, DB);
    }

    /**
     * 查询
     * @param map
     * @return
     */
    public static JSONObject select(Map<String, Object> map) {
        String tableName = (String) map.get("tableName");
        String queryJson = (String) map.get("queryJson");
        Query query = new BasicQuery(queryJson);

        List<JSONObject> result = template.find(query, JSONObject.class, tableName);
        if (result.size() > 0) {
            return result.get(0);
        }
        return null;
    }

    /**
     * 插入
     * @param map
     * @param <E>
     * @throws Exception
     */
    public static <E> void batchInsert(Map<String, Collection<E>> map) throws Exception {
        for (String key : map.keySet()) {
            template.insert(map.get(key), key);
        }
    }

    public static void main(String[] args) throws Exception {

        // 可以指定_id,指定了就用自己的id而不是数据库生成的id
        String json1 = "{\n" +
                "\t\"uniscId\": \"91211103MA10E6140W\",\n" +
                "\t\"entTypeCN\": \"有限责任公司（自然人投资或控股）\",\n" +
                "\t\"entName\": \"盘锦信朋缘劳务有限公司\",\n" +
                "\t\"corpStatusString\": \"存续（在营、开业、在册）\",\n" +
                "\t\"estDate\": \"2020-06-04\",\n" +
                "\t\"apprDate\": \"2020-06-04\",\n" +
                "\t\"regOrgCn\": \"盘锦市兴隆台区市场监督管理局\",\n" +
                "\t\"regCapCurCN\": \"人民币\",\n" +
                "\t\"regCaption\": 100.0,\n" +
                "\t\"regCap\": \"壹佰万元整\",\n" +
                "\t\"legalName\": \"张艳阳\",\n" +
                "\t\"opFrom\": \"2020-06-04\",\n" +
                "\t\"opTo\": \"2040-06-03\",\n" +
                "\t\"regProv\": \"辽宁省\",\n" +
                "\t\"regCity\": \"盘锦市\",\n" +
                "\t\"regDistrict\": \"兴隆台区\",\n" +
                "\t\"opScope\": \"许可项目：建筑劳务分包，住宅室内装饰装修，各类工程建设活动，消防设施工程（依法须经批准的项目，经相关部门批准后方可开展经营活动，具体经营项目以审批结果为准） 一般项目：园林绿化工程施工，家政服务，五金产品零售，日用百货销售，家用电器销售，建筑装饰材料销售，机械设备租赁，消防器材销售（除依法须经批准的项目外，凭营业执照依法自主开展经营活动）\",\n" +
                "\t\"dom\": \"辽宁省盘锦市兴隆台区泰山路东、大众街北王家荒回迁楼3#11003\",\n" +
                "\t\"source\": \"爱企查公众号\",\n" +
                "\t\"spider\": \"aiqichaapp\",\n" +
                "\t\"q\": 91,\n" +
                "\t\"hidden\": 0,\n" +
                "\t\"createdAt\": 1602580239,\n" +
                "\t\"entId\": \"5f856f0f8034b3c2d705f647\",\n" +
                "\t\"desc\": \"盘锦信朋缘劳务有限公司成立于2020年6月4日，法定代表人为张艳阳，注册资本为100.0万元,统一社会信用代码为91211103MA10E6140W，企业地址位于辽宁省盘锦市兴隆台区泰山路东、大众街北王家荒回迁楼3#11003，经营范围包括：许可项目：建筑劳务分包，住宅室内装饰装修，各类工程建设活动，消防设施工程（依法须经批准的项目，经相关部门批准后方可开展经营活动，具体经营项目以审批结果为准） 一般项目：园林绿化工程施工，家政服务，五金产品零售，日用百货销售，家用电器销售，建筑装饰材料销售，机械设备租赁，消防器材销售（除依法须经批准的项目外，凭营业执照依法自主开展经营活动）。盘锦信朋缘劳务有限公司目前的经营状态为存续（在营、开业、在册）。\"\n" +
                "}";

        ArrayList<JSONObject> list = new ArrayList<>();
        JSONObject jsonObj1 = JSONObject.parseObject(json1);
        jsonObj1.put("_id","333");
        list.add(jsonObj1);

//        ArrayList<String> list = new ArrayList<>();
//        list.add(json1);
//        batchInsertListNew(list);
    }

    /**
     * Mongo插入方法,推荐使用springDataMongoAPI，插入数据可以不用转换格式，只要是json数据就可以，
     * 而mongoDB的原生API就不行，需要转成Document元素格式才可以执行
     *
     * list可以是任意格式的json数据
     * @param list
     * @param <E>
     * @throws Exception
     */
    public static <E> void batchInsertListNew(ArrayList<JSONObject> list) throws Exception {
        template.insert(list,"test");
//        template.updateMulti()
//        template.upsert()
    }

    public static <E> void batchInsertList(ArrayList<JSONObject> list) throws Exception {
        Map<String, Collection<JSONObject>> map = new HashMap<String, Collection<JSONObject>>();
        map.put("ent_table_count_1", list);
        for (String key : map.keySet()) {
            template.insert(map.get(key), key);
        }
    }

    public static void insertJson(JSONObject json) {
        if (json == null || json.keySet().size() == 0) {
            return;
        }
        try {
            Map<String, Collection<JSONObject>> map = new HashMap<String, Collection<JSONObject>>();

            for (String key : json.keySet()) {
                List<JSONObject> list = new ArrayList<JSONObject>();
                JSONArray jsonArray = json.getJSONArray(key);
                for (int i = 0; i < jsonArray.size(); i++) {
                    list.add(jsonArray.getJSONObject(i));
                }
                map.put(key, list);
            }
            batchInsert(map);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    /**
     * 更新并插入
     * @param map
     */
    public static void upsert(Map<String, Object> map) {
        String tableName = (String) map.get("tableName");
        String queryJson = (String) map.get("queryJson");
        Query query = new BasicQuery(queryJson);
        Update update = new Update();
        String updateJson = (String) map.get("updateJson");
        org.bson.Document doc = JSONObject.parseObject(updateJson, Document.class);
        template.upsert(query, Update.fromDocument(doc, ""), tableName);
    }

    public static void upsertJson(JSONObject json) {
        for (String key : json.keySet()) {
            if (TableName.MANGO_ENT.equals(key)) {
                JSONObject ent = json.getJSONArray(key).getJSONObject(0);
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("tableName", "ent");
                JSONObject queryJson = new JSONObject();
                queryJson.put("entId", ent.getString("entId"));
                map.put("queryJson", queryJson.toJSONString());
                map.put("updateJson", ent.toJSONString());
                upsert(map);
            }
        }
    }

    // 把标签字段写入mongo
    public static void updateSystemTags(JSONObject json){
        for (String key : json.keySet()) {
            if (TableName.MANGO_ENT.equals(key)) {
                JSONArray currentArr = json.getJSONArray(key);
                for (int i = 0; i < currentArr.size(); i++) {
                    JSONObject currentJson = currentArr.getJSONObject(i);

                    if (StringUtils.isNotEmpty(currentJson.getString("systemTags"))){
                        Query query = new Query();
                        query.addCriteria(Criteria.where("_id").is(new ObjectId(currentJson.getString("entId"))));
                        Update update = new Update();
                        update.set("systemTags",currentJson.getString("systemTags"));
//                        update.unset("systemTags");
                        template.updateFirst(query,update,TableName.MANGO_ENT);
                    }
                }
            }
        }
    }
}
