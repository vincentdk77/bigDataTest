package com.atguigu.kemai.entity.mango;


public class EntCert {
//     """
//    资质证书
//    """
    private String certNo ;//    # 证书编号
    private String certType ;//   # 资质类别
    private String pubDate ;//    # 颁布日期
    private String expireDate ;//  # 有效期
    private String org ;//       # 发证机构
    private String entName ;//
    private String source ;//
    private String sourceUrl ;//
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

    public String getCertNo() {
        return certNo;
    }

    public void setCertNo(String certNo) {
        this.certNo = certNo;
    }

    public String getCertType() {
        return certType;
    }

    public void setCertType(String certType) {
        this.certType = certType;
    }

    public String getPubDate() {
        return pubDate;
    }

    public void setPubDate(String pubDate) {
        this.pubDate = pubDate;
    }

    public String getExpireDate() {
        return expireDate;
    }

    public void setExpireDate(String expireDate) {
        this.expireDate = expireDate;
    }

    public String getOrg() {
        return org;
    }

    public void setOrg(String org) {
        this.org = org;
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

    @Override
    public String toString() {
        return "EntCert{" +
                "certNo='" + certNo + '\'' +
                ", certType='" + certType + '\'' +
                ", pubDate='" + pubDate + '\'' +
                ", expireDate='" + expireDate + '\'' +
                ", org='" + org + '\'' +
                ", entName='" + entName + '\'' +
                ", source='" + source + '\'' +
                ", sourceUrl='" + sourceUrl + '\'' +
                ", spider='" + spider + '\'' +
                '}';
    }
}
