package com.atguigu.kemai.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;

public class TableCountHandle {

    /**
     * 求每一个表字段的个数
     * ent表：求第一个ent的字段key的个数和更新时间
     * 其他表：求该表对应的json数组的长度即可
     *
     * @param json
     * @return
     */
    public static JSONObject count(JSONObject json) {
        JSONObject newJson = new JSONObject();

        for (String key : json.keySet()) {
            if ("ent".equals(key)) {
                JSONObject ent = json.getJSONArray(key).getJSONObject(0);

                newJson.put("_id", new ObjectId(ent.getString("entId")));
                newJson.put("entId", ent.getString("entId"));
                newJson.put("ent_field_count", ent.keySet().size());

                String updateTime = null;
                if (!StringUtils.isEmpty(ent.getString("updatedAt"))) {
                    updateTime = ent.getString("updatedAt");
                } else if (!StringUtils.isEmpty(ent.getString("createdAt"))) {
                    updateTime = ent.getString("createdAt");
                }
                newJson.put("ent_time", updateTime);
            } else {
                JSONArray noEntArray = json.getJSONArray(key);
                newJson.put(key, noEntArray.size());
            }
        }
        return newJson;
    }

    public static JSONObject countFields(JSONObject json) {
        JSONObject newJson = new JSONObject();
        newJson.put("entId", json.getJSONArray("ent").getJSONObject(0).getString("entId"));
        newJson.put("counts",json.keySet().size());
        return newJson;
    }
}
