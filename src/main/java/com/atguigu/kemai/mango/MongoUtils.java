package com.atguigu.kemai.mango;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.kemai.utils.TableName;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.*;


public class MongoUtils {
    private static final MongoTemplate template;

    private static final String DB = "ent";
    static {
        MongoClient client = new MongoClient(
                new ServerAddress("node11", 28018),
                MongoCredential.createCredential("spark", DB, "spark$890$3".toCharArray()),
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
