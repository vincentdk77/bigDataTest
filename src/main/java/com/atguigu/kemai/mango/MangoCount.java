package com.atguigu.kemai.mango;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.kemai.utils.TableName;
import org.bson.types.ObjectId;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

public class MangoCount {
    public static Integer YES = 0;
    public static Integer No = 1;

    public static JSONObject jsonCount(JSONObject json) {
        JSONObject newJson = new JSONObject();
        Set<String> keys = json.keySet();

        try {
            for (String key : keys) {
                if (TableName.MANGO_ENT.equals(key)) {
                    JSONObject ent = json.getJSONArray(key).getJSONObject(0);

                    newJson.put("_id", new ObjectId(ent.getString("entId")));
                    newJson.put("entId", ent.getString("entId"));
                    newJson.put("ent_field_count", ent.keySet().size());    //Ent表字段数

                    String updateTime = null;   //更新时间
                    if (!StringUtils.isEmpty(ent.getString("updatedAt"))) {
                        updateTime = ent.getString("updatedAt");
                    } else if (!StringUtils.isEmpty(ent.getString("createdAt"))) {
                        updateTime = ent.getString("createdAt");
                    }
                    newJson.put("ent_time", updateTime);
                } else {
                    JSONArray noEntArray = json.getJSONArray(key);

                    String updateTime = null;
                    if (!StringUtils.isEmpty(noEntArray.getJSONObject(0).getString("updatedAt"))) {
                        updateTime = noEntArray.getJSONObject(0).getString("updatedAt");
                        for (int i = 1; i < noEntArray.size(); i++) {
                            JSONObject noEnt = noEntArray.getJSONObject(i);
                            if (!StringUtils.isEmpty(noEntArray.getJSONObject(i).getString("updatedAt")) &&    //比较的值不可为null，需判断
                                    updateTime.compareTo(noEnt.getString("updatedAt")) < 0) {
                                updateTime = noEnt.getString("updatedAt");
                            }
                        }
                    } else if (!StringUtils.isEmpty(noEntArray.getJSONObject(0).getString("createdAt"))) {
                        updateTime = noEntArray.getJSONObject(0).getString("createdAt");
                        for (int i = 1; i < noEntArray.size(); i++) {
                            JSONObject noEnt = noEntArray.getJSONObject(i);
                            if (!StringUtils.isEmpty(noEntArray.getJSONObject(i).getString("createdAt")) &&
                                    updateTime.compareTo(noEnt.getString("createdAt")) < 0) {
                                updateTime = noEnt.getString("createdAt");
                            }
                        }
                    }
                    newJson.put(key, noEntArray.size());        //指定entId在其它表的记录条数
                    newJson.put(key + "_time", updateTime);
                }
            }
        } catch (Exception e) {
            System.out.println(e + " " + json.toJSONString());
        }
        return newJson;
    }
}
