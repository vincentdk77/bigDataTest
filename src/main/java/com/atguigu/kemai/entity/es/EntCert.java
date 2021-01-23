package com.atguigu.kemai.entity.es;

public class EntCert {
//     """
//    资质证书
//    """
    private String entId;

    private String certType ;//   # 资质类别
    private String certNo ;//    # 证书编号
    private String pubDate ;//    # 颁布日期
    private String expireDate ;//  # 有效期

    private String certTypeLTM; //最近3个月内截止证书类别

    private String certTypeLSM; //最近6个月内截止证书类别

    @Override
    public String toString() {
        return "EntCert{" +
                "entId='" + entId + '\'' +
                ", certType='" + certType + '\'' +
                ", certNo='" + certNo + '\'' +
                ", pubDate='" + pubDate + '\'' +
                ", expireDate='" + expireDate + '\'' +
                ", certTypeLTM='" + certTypeLTM + '\'' +
                ", certTypeLSM='" + certTypeLSM + '\'' +
                '}';
    }

    public String getEntId() {
        return entId;
    }

    public void setEntId(String entId) {
        this.entId = entId;
    }

    public String getCertType() {
        return certType;
    }

    public void setCertType(String certType) {
        this.certType = certType;
    }

    public String getCertNo() {
        return certNo;
    }

    public void setCertNo(String certNo) {
        this.certNo = certNo;
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

    public String getCertTypeLTM() {
        return certTypeLTM;
    }

    public void setCertTypeLTM(String certTypeLTM) {
        this.certTypeLTM = certTypeLTM;
    }

    public String getCertTypeLSM() {
        return certTypeLSM;
    }

    public void setCertTypeLSM(String certTypeLSM) {
        this.certTypeLSM = certTypeLSM;
    }
}
