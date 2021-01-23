package com.atguigu.kemai.recommend;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.util.List;

/**
 * 4级：A-01-011-0111
 */
public class CategoryItem implements Serializable {

    private String id;
    private String name;
    private List<CategoryItem> childs;  // chuangrui 包有使用，暂不改动

    //新增字段（递归分类用）
    private String pid;
    private JSONObject parent;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<CategoryItem> getChilds() {
        return childs;
    }

    public void setChilds(List<CategoryItem> childs) {
        this.childs = childs;
    }

    public JSONObject getParent() {
        return parent;
    }

    public void setParent(JSONObject parent) {
        this.parent = parent;
    }
}