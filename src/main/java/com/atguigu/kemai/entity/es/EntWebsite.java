package com.atguigu.kemai.entity.es;

public class EntWebsite {

    //首页布局
    private String  indexLayout;

    private String entId;
    private String domainKeyword;//关键词

    private String domainDesc;//官网描述

    private String siteContent;//网站内容

    private String onlineServiceType;//客服类型



    private String domainTitle;//网站标题

    private String w3cDeviate;//W3C标准偏离值

    private String ipNodes;//IP运营节点

    private String ipAddress;//IP物理位置

    private String soWeight;//360权重

    private String baiduIndex;//百度收录量

    private String soIndex;//360收录量

    private String domainTech;//网站技术

    private String domainExpDate;//域名到期时间

    @Override
    public String toString() {
        return "EntWebsite{" +
                "entId='" + entId + '\'' +
                ", domainKeyword='" + domainKeyword + '\'' +
                ", domainDesc='" + domainDesc + '\'' +
                ", siteContent='" + siteContent + '\'' +
                ", onlineServiceType='" + onlineServiceType + '\'' +
                ", indexLayout='" + indexLayout + '\'' +
                ", domainTitle='" + domainTitle + '\'' +
                ", w3cDeviate='" + w3cDeviate + '\'' +
                ", ipNodes='" + ipNodes + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", soWeight='" + soWeight + '\'' +
                ", baiduIndex='" + baiduIndex + '\'' +
                ", soIndex='" + soIndex + '\'' +
                ", domainTech='" + domainTech + '\'' +
                ", domainExpDate='" + domainExpDate + '\'' +
                '}';
    }



    public String getEntId() {
        return entId;
    }

    public void setEntId(String entId) {
        this.entId = entId;
    }

    public String getDomainKeyword() {
        return domainKeyword;
    }

    public void setDomainKeyword(String domainKeyword) {
        this.domainKeyword = domainKeyword;
    }

    public String getDomainDesc() {
        return domainDesc;
    }

    public void setDomainDesc(String domainDesc) {
        this.domainDesc = domainDesc;
    }

    public String getSiteContent() {
        return siteContent;
    }

    public void setSiteContent(String siteContent) {
        this.siteContent = siteContent;
    }

    public String getOnlineServiceType() {
        return onlineServiceType;
    }

    public void setOnlineServiceType(String onlineServiceType) {
        this.onlineServiceType = onlineServiceType;
    }

    public String getIndexLayout() {
        return indexLayout;
    }

    public void setIndexLayout(String indexLayout) {
        this.indexLayout = indexLayout;
    }

    public String getDomainTitle() {
        return domainTitle;
    }

    public void setDomainTitle(String domainTitle) {
        this.domainTitle = domainTitle;
    }

    public String getW3cDeviate() {
        return w3cDeviate;
    }

    public void setW3cDeviate(String w3cDeviate) {
        this.w3cDeviate = w3cDeviate;
    }

    public String getIpNodes() {
        return ipNodes;
    }

    public void setIpNodes(String ipNodes) {
        this.ipNodes = ipNodes;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getSoWeight() {
        return soWeight;
    }

    public void setSoWeight(String soWeight) {
        this.soWeight = soWeight;
    }

    public String getBaiduIndex() {
        return baiduIndex;
    }

    public void setBaiduIndex(String baiduIndex) {
        this.baiduIndex = baiduIndex;
    }

    public String getSoIndex() {
        return soIndex;
    }

    public void setSoIndex(String soIndex) {
        this.soIndex = soIndex;
    }

    public String getDomainTech() {
        return domainTech;
    }

    public void setDomainTech(String domainTech) {
        this.domainTech = domainTech;
    }

    public String getDomainExpDate() {
        return domainExpDate;
    }

    public void setDomainExpDate(String domainExpDate) {
        this.domainExpDate = domainExpDate;
    }
}
