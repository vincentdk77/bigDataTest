package com.atguigu.kemai.entity.mango;

public class EntEquityPledged {
    //     """
//    股权出质
//    """
    private String entName;//
    private String equityNo;// # 股权登记编号
    private String impam;// # 出质股权数额 (数值，小数万元)
    private String currencyCN;// # 出质股权数额币种（中文名称）
    private String impOrg;// # 质权人
    private String regDate;// # 股权出质登记日期

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
        return "EntEquityPledged{" +
                "entName='" + entName + '\'' +
                ", equityNo='" + equityNo + '\'' +
                ", impam='" + impam + '\'' +
                ", currencyCN='" + currencyCN + '\'' +
                ", impOrg='" + impOrg + '\'' +
                ", regDate='" + regDate + '\'' +
                ", source='" + source + '\'' +
                ", sourceUrl='" + sourceUrl + '\'' +
                ", spider='" + spider + '\'' +
                '}';
    }

    public String getEntName() {
        return entName;
    }

    public void setEntName(String entName) {
        this.entName = entName;
    }

    public String getEquityNo() {
        return equityNo;
    }

    public void setEquityNo(String equityNo) {
        this.equityNo = equityNo;
    }

    public String getImpam() {
        return impam;
    }

    public void setImpam(String impam) {
        this.impam = impam;
    }

    public String getCurrencyCN() {
        return currencyCN;
    }

    public void setCurrencyCN(String currencyCN) {
        this.currencyCN = currencyCN;
    }

    public String getImpOrg() {
        return impOrg;
    }

    public void setImpOrg(String impOrg) {
        this.impOrg = impOrg;
    }

    public String getRegDate() {
        return regDate;
    }

    public void setRegDate(String regDate) {
        this.regDate = regDate;
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
