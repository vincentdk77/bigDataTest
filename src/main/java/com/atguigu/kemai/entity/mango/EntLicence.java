package com.atguigu.kemai.entity.mango;

public class EntLicence {
//    """行政许可

    private String uniscId;// # 统一信用编码
    private String entName;// # 企业名称
    private String licId;//  # 编号： 443DC079B415247BE050000A0CC882A3
    private String pripId;//

    private String licNo;// # 许可编号： SCXK(皖)2016-001
    private String licName;//
    private String licNameCN;// # 许可名称：实验动物许可证
    private String valFrom;//# 有效期自：int
    private String valTo;// # 有效值至 ： int
    private String licAnth;//# 许可机关
    private String licItem;// # 许可内容
    private String status;// # 状态数值：1
    private String statusCN;//# 状态描述：
    private String canDate;//
    private String equPleCanRea;//
    private String revDate;//

    private String sugRevReason;//
    private String invalidDate;//
    private String invalidRea;//

    private String altId;//
    private String alt;//
    private String altBe;//
    private String altAf;//
    private String altDate;//

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
        return "EntLicence{" +
                "uniscId='" + uniscId + '\'' +
                ", entName='" + entName + '\'' +
                ", licId='" + licId + '\'' +
                ", pripId='" + pripId + '\'' +
                ", licNo='" + licNo + '\'' +
                ", licName='" + licName + '\'' +
                ", licNameCN='" + licNameCN + '\'' +
                ", valFrom='" + valFrom + '\'' +
                ", valTo='" + valTo + '\'' +
                ", licAnth='" + licAnth + '\'' +
                ", licItem='" + licItem + '\'' +
                ", status='" + status + '\'' +
                ", statusCN='" + statusCN + '\'' +
                ", canDate='" + canDate + '\'' +
                ", equPleCanRea='" + equPleCanRea + '\'' +
                ", revDate='" + revDate + '\'' +
                ", sugRevReason='" + sugRevReason + '\'' +
                ", invalidDate='" + invalidDate + '\'' +
                ", invalidRea='" + invalidRea + '\'' +
                ", altId='" + altId + '\'' +
                ", alt='" + alt + '\'' +
                ", altBe='" + altBe + '\'' +
                ", altAf='" + altAf + '\'' +
                ", altDate='" + altDate + '\'' +
                ", source='" + source + '\'' +
                ", sourceUrl='" + sourceUrl + '\'' +
                ", spider='" + spider + '\'' +
                '}';
    }

    public String getUniscId() {
        return uniscId;
    }

    public void setUniscId(String uniscId) {
        this.uniscId = uniscId;
    }

    public String getEntName() {
        return entName;
    }

    public void setEntName(String entName) {
        this.entName = entName;
    }

    public String getLicId() {
        return licId;
    }

    public void setLicId(String licId) {
        this.licId = licId;
    }

    public String getPripId() {
        return pripId;
    }

    public void setPripId(String pripId) {
        this.pripId = pripId;
    }

    public String getLicNo() {
        return licNo;
    }

    public void setLicNo(String licNo) {
        this.licNo = licNo;
    }

    public String getLicName() {
        return licName;
    }

    public void setLicName(String licName) {
        this.licName = licName;
    }

    public String getLicNameCN() {
        return licNameCN;
    }

    public void setLicNameCN(String licNameCN) {
        this.licNameCN = licNameCN;
    }

    public String getValFrom() {
        return valFrom;
    }

    public void setValFrom(String valFrom) {
        this.valFrom = valFrom;
    }

    public String getValTo() {
        return valTo;
    }

    public void setValTo(String valTo) {
        this.valTo = valTo;
    }

    public String getLicAnth() {
        return licAnth;
    }

    public void setLicAnth(String licAnth) {
        this.licAnth = licAnth;
    }

    public String getLicItem() {
        return licItem;
    }

    public void setLicItem(String licItem) {
        this.licItem = licItem;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStatusCN() {
        return statusCN;
    }

    public void setStatusCN(String statusCN) {
        this.statusCN = statusCN;
    }

    public String getCanDate() {
        return canDate;
    }

    public void setCanDate(String canDate) {
        this.canDate = canDate;
    }

    public String getEquPleCanRea() {
        return equPleCanRea;
    }

    public void setEquPleCanRea(String equPleCanRea) {
        this.equPleCanRea = equPleCanRea;
    }

    public String getRevDate() {
        return revDate;
    }

    public void setRevDate(String revDate) {
        this.revDate = revDate;
    }

    public String getSugRevReason() {
        return sugRevReason;
    }

    public void setSugRevReason(String sugRevReason) {
        this.sugRevReason = sugRevReason;
    }

    public String getInvalidDate() {
        return invalidDate;
    }

    public void setInvalidDate(String invalidDate) {
        this.invalidDate = invalidDate;
    }

    public String getInvalidRea() {
        return invalidRea;
    }

    public void setInvalidRea(String invalidRea) {
        this.invalidRea = invalidRea;
    }

    public String getAltId() {
        return altId;
    }

    public void setAltId(String altId) {
        this.altId = altId;
    }

    public String getAlt() {
        return alt;
    }

    public void setAlt(String alt) {
        this.alt = alt;
    }

    public String getAltBe() {
        return altBe;
    }

    public void setAltBe(String altBe) {
        this.altBe = altBe;
    }

    public String getAltAf() {
        return altAf;
    }

    public void setAltAf(String altAf) {
        this.altAf = altAf;
    }

    public String getAltDate() {
        return altDate;
    }

    public void setAltDate(String altDate) {
        this.altDate = altDate;
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
