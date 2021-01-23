package com.atguigu.kemai.entity.mango;


//专利
public class EntPatent {
    private String name;// #名称
    private String summary;// #
    private String appDate;// # 申请日期
    private String appNo;//  # 申请号
    private String appAddr;// # 申请人地址
    private String ipc;//    # ipc 列表[]
    private String pubDate;// # 公开日期
    private String pubNo;//  # 公开号
    private String type;//  # 类型： 发明申请/外观设计/实用新型/发明授权
    private String inventor;// # 发明人列表: []
    private String agent;//  # 代理人列表： []
    private String agency;// # 代理机构名称
    private String priorityNo;// # 优先权号
    private String priorityDate;// # 优先权日期
    private String entName;//

    private String source;//
    private String sourceUrl;//
    private String spider;//

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
        return "EntPatent{" +
                "name='" + name + '\'' +
                ", summary='" + summary + '\'' +
                ", appDate='" + appDate + '\'' +
                ", appNo='" + appNo + '\'' +
                ", appAddr='" + appAddr + '\'' +
                ", ipc='" + ipc + '\'' +
                ", pubDate='" + pubDate + '\'' +
                ", pubNo='" + pubNo + '\'' +
                ", type='" + type + '\'' +
                ", inventor='" + inventor + '\'' +
                ", agent='" + agent + '\'' +
                ", agency='" + agency + '\'' +
                ", priorityNo='" + priorityNo + '\'' +
                ", priorityDate='" + priorityDate + '\'' +
                ", entName='" + entName + '\'' +
                ", source='" + source + '\'' +
                ", sourceUrl='" + sourceUrl + '\'' +
                ", spider='" + spider + '\'' +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public String getAppDate() {
        return appDate;
    }

    public void setAppDate(String appDate) {
        this.appDate = appDate;
    }

    public String getAppNo() {
        return appNo;
    }

    public void setAppNo(String appNo) {
        this.appNo = appNo;
    }

    public String getAppAddr() {
        return appAddr;
    }

    public void setAppAddr(String appAddr) {
        this.appAddr = appAddr;
    }

    public String getIpc() {
        return ipc;
    }

    public void setIpc(String ipc) {
        this.ipc = ipc;
    }

    public String getPubDate() {
        return pubDate;
    }

    public void setPubDate(String pubDate) {
        this.pubDate = pubDate;
    }

    public String getPubNo() {
        return pubNo;
    }

    public void setPubNo(String pubNo) {
        this.pubNo = pubNo;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getInventor() {
        return inventor;
    }

    public void setInventor(String inventor) {
        this.inventor = inventor;
    }

    public String getAgent() {
        return agent;
    }

    public void setAgent(String agent) {
        this.agent = agent;
    }

    public String getAgency() {
        return agency;
    }

    public void setAgency(String agency) {
        this.agency = agency;
    }

    public String getPriorityNo() {
        return priorityNo;
    }

    public void setPriorityNo(String priorityNo) {
        this.priorityNo = priorityNo;
    }

    public String getPriorityDate() {
        return priorityDate;
    }

    public void setPriorityDate(String priorityDate) {
        this.priorityDate = priorityDate;
    }

    public String getEntName() {
        return entName;
    }

    public void setEntName(String entName) {
        this.entName = entName;
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
}
