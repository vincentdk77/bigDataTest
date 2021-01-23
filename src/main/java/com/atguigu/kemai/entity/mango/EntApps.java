package com.atguigu.kemai.entity.mango;

public class EntApps {

    private String appCategory;
    private String appDesc;
    private String appDownloadCount;
    private String appIconUri;
    private String appName;
    private String appOS;
    private String appScore;
    private String appSize;
    private String appStutus;
    private String appStores;
    private String appUpgradeTime;
    private String appUrl;
    private String source;
    private String sourceUrl;
    private String spider;
    //   # 全站抓取时关联用
    private String entName;

    private String createTime;
    private String updateTime;

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public String toString() {
        return "EntApps{" +
                "appCategory='" + appCategory + '\'' +
                ", appDesc='" + appDesc + '\'' +
                ", appDownloadCount='" + appDownloadCount + '\'' +
                ", appIconUri='" + appIconUri + '\'' +
                ", appName='" + appName + '\'' +
                ", appOS='" + appOS + '\'' +
                ", appScore='" + appScore + '\'' +
                ", appSize='" + appSize + '\'' +
                ", appStutus='" + appStutus + '\'' +
                ", appStores='" + appStores + '\'' +
                ", appUpgradeTime='" + appUpgradeTime + '\'' +
                ", appUrl='" + appUrl + '\'' +
                ", source='" + source + '\'' +
                ", sourceUrl='" + sourceUrl + '\'' +
                ", spider='" + spider + '\'' +
                ", entName='" + entName + '\'' +
                '}';
    }

    public String getAppCategory() {
        return appCategory;
    }

    public void setAppCategory(String appCategory) {
        this.appCategory = appCategory;
    }

    public String getAppDesc() {
        return appDesc;
    }

    public void setAppDesc(String appDesc) {
        this.appDesc = appDesc;
    }

    public String getAppDownloadCount() {
        return appDownloadCount;
    }

    public void setAppDownloadCount(String appDownloadCount) {
        this.appDownloadCount = appDownloadCount;
    }

    public String getAppIconUri() {
        return appIconUri;
    }

    public void setAppIconUri(String appIconUri) {
        this.appIconUri = appIconUri;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getAppOS() {
        return appOS;
    }

    public void setAppOS(String appOS) {
        this.appOS = appOS;
    }

    public String getAppScore() {
        return appScore;
    }

    public void setAppScore(String appScore) {
        this.appScore = appScore;
    }

    public String getAppSize() {
        return appSize;
    }

    public void setAppSize(String appSize) {
        this.appSize = appSize;
    }

    public String getAppStutus() {
        return appStutus;
    }

    public void setAppStutus(String appStutus) {
        this.appStutus = appStutus;
    }

    public String getAppStores() {
        return appStores;
    }

    public void setAppStores(String appStores) {
        this.appStores = appStores;
    }

    public String getAppUpgradeTime() {
        return appUpgradeTime;
    }

    public void setAppUpgradeTime(String appUpgradeTime) {
        this.appUpgradeTime = appUpgradeTime;
    }

    public String getAppUrl() {
        return appUrl;
    }

    public void setAppUrl(String appUrl) {
        this.appUrl = appUrl;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSourceUrl() {
        return sourceUrl;
    }

    public void setSourceUrl(String sourceUrl) {
        this.sourceUrl = sourceUrl;
    }

    public String getSpider() {
        return spider;
    }

    public void setSpider(String spider) {
        this.spider = spider;
    }

    public String getEntName() {
        return entName;
    }

    public void setEntName(String entName) {
        this.entName = entName;
    }
}
