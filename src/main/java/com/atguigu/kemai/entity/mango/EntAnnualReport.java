package com.atguigu.kemai.entity.mango;


//年报
public class EntAnnualReport {
    private String uniscId;
    private String pripId;
    private String anCheYear;
    private String annRepDetailUrl;
    private String anCheId;
    private String entType;
    private String nodeNum;
    //         # 年报内容
    private String raw;
    //# 全站抓取时关联用

    private String pubDate;
    private String entName;


    private String createTime;
    private String updateTime;

    public String getPubDate() {
        return pubDate;
    }

    public void setPubDate(String pubDate) {
        this.pubDate = pubDate;
    }

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
        return "EntAnnualReport{" +
                "uniscId='" + uniscId + '\'' +
                ", pripId='" + pripId + '\'' +
                ", anCheYear='" + anCheYear + '\'' +
                ", annRepDetailUrl='" + annRepDetailUrl + '\'' +
                ", anCheId='" + anCheId + '\'' +
                ", entType='" + entType + '\'' +
                ", nodeNum='" + nodeNum + '\'' +
                ", raw='" + raw + '\'' +
                ", entName='" + entName + '\'' +
                '}';
    }

    public String getUniscId() {
        return uniscId;
    }

    public void setUniscId(String uniscId) {
        this.uniscId = uniscId;
    }

    public String getPripId() {
        return pripId;
    }

    public void setPripId(String pripId) {
        this.pripId = pripId;
    }

    public String getAnCheYear() {
        return anCheYear;
    }

    public void setAnCheYear(String anCheYear) {
        this.anCheYear = anCheYear;
    }

    public String getAnnRepDetailUrl() {
        return annRepDetailUrl;
    }

    public void setAnnRepDetailUrl(String annRepDetailUrl) {
        this.annRepDetailUrl = annRepDetailUrl;
    }

    public String getAnCheId() {
        return anCheId;
    }

    public void setAnCheId(String anCheId) {
        this.anCheId = anCheId;
    }

    public String getEntType() {
        return entType;
    }

    public void setEntType(String entType) {
        this.entType = entType;
    }

    public String getNodeNum() {
        return nodeNum;
    }

    public void setNodeNum(String nodeNum) {
        this.nodeNum = nodeNum;
    }

    public String getRaw() {
        return raw;
    }

    public void setRaw(String raw) {
        this.raw = raw;
    }

    public String getEntName() {
        return entName;
    }

    public void setEntName(String entName) {
        this.entName = entName;
    }
}
