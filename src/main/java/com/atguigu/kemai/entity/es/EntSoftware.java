package com.atguigu.kemai.entity.es;

public class EntSoftware {
    private String entId;//
    private String appNames;//  #名称
    private String appDesc; //
    private String appCate;// #应用分类
    private String appStore;//应用上架平台
    private String appPublishTime;//最近发布时间
    private String appUpgradeTime;//最近更新时间

    @Override
    public String toString() {
        return "EntSoftware{" +
                "entId='" + entId + '\'' +
                ", appNames='" + appNames + '\'' +
                ", appDesc='" + appDesc + '\'' +
                ", appCate='" + appCate + '\'' +
                ", appStore='" + appStore + '\'' +
                ", appPublishTime='" + appPublishTime + '\'' +
                ", appUpgradeTime='" + appUpgradeTime + '\'' +
                '}';
    }

    public String getEntId() {
        return entId;
    }

    public void setEntId(String entId) {
        this.entId = entId;
    }

    public String getAppNames() {
        return appNames;
    }

    public void setAppNames(String appNames) {
        this.appNames = appNames;
    }

    public String getAppDesc() {
        return appDesc;
    }

    public void setAppDesc(String appDesc) {
        this.appDesc = appDesc;
    }

    public String getAppCate() {
        return appCate;
    }

    public void setAppCate(String appCate) {
        this.appCate = appCate;
    }

    public String getAppStore() {
        return appStore;
    }

    public void setAppStore(String appStore) {
        this.appStore = appStore;
    }

    public String getAppPublishTime() {
        return appPublishTime;
    }

    public void setAppPublishTime(String appPublishTime) {
        this.appPublishTime = appPublishTime;
    }

    public String getAppUpgradeTime() {
        return appUpgradeTime;
    }

    public void setAppUpgradeTime(String appUpgradeTime) {
        this.appUpgradeTime = appUpgradeTime;
    }
}
