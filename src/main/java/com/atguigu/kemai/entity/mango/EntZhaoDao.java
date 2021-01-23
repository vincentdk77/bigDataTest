package com.atguigu.kemai.entity.mango;

public class EntZhaoDao {
    private String name;

    private String cid;
    private String createdTime;//# 创建时间 时间戳
    private String lastActive;// # 最后活跃时间 时间戳
    private String position;//  # 职位
    private String authFlag;//  # 是否认证
    private String entName;//

    private String company;

    private String avatar;// 头像
    private String dep;//  # 部门
    private String cpId;//  # 主页链接
    private String cpJobFunction;//  # 行业 ：管理|项目|销售

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    public String getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(String createdTime) {
        this.createdTime = createdTime;
    }

    public String getLastActive() {
        return lastActive;
    }

    public void setLastActive(String lastActive) {
        this.lastActive = lastActive;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getAuthFlag() {
        return authFlag;
    }

    public void setAuthFlag(String authFlag) {
        this.authFlag = authFlag;
    }

    public String getEntName() {
        return entName;
    }

    public void setEntName(String entName) {
        this.entName = entName;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getAvatar() {
        return avatar;
    }

    public void setAvatar(String avatar) {
        this.avatar = avatar;
    }

    public String getDep() {
        return dep;
    }

    public void setDep(String dep) {
        this.dep = dep;
    }

    public String getCpId() {
        return cpId;
    }

    public void setCpId(String cpId) {
        this.cpId = cpId;
    }

    public String getCpJobFunction() {
        return cpJobFunction;
    }

    public void setCpJobFunction(String cpJobFunction) {
        this.cpJobFunction = cpJobFunction;
    }
}

