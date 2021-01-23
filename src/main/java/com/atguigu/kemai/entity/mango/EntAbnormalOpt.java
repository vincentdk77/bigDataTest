package com.atguigu.kemai.entity.mango;

public class EntAbnormalOpt {
    //    """经营异常
//            """
    private String uniscId;//
    // # 异常内容
    private String speCauseCN;//
    // # 异常时间
    private String abntime;//
    //# 市场管理局
    private String decOrgCN;//
    // # 移除原因
    private String remExcpResCN;//
    //# 移除日期
    private String remDate;//
    private String reDecOrgCN;//
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
        return "EntAbnormalOpt{" +
                "uniscId='" + uniscId + '\'' +
                ", speCauseCN='" + speCauseCN + '\'' +
                ", abntime='" + abntime + '\'' +
                ", decOrgCN='" + decOrgCN + '\'' +
                ", remExcpResCN='" + remExcpResCN + '\'' +
                ", remDate='" + remDate + '\'' +
                ", reDecOrgCN='" + reDecOrgCN + '\'' +
                ", entName='" + entName + '\'' +
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

    public String getSpeCauseCN() {
        return speCauseCN;
    }

    public void setSpeCauseCN(String speCauseCN) {
        this.speCauseCN = speCauseCN;
    }

    public String getAbntime() {
        return abntime;
    }

    public void setAbntime(String abntime) {
        this.abntime = abntime;
    }

    public String getDecOrgCN() {
        return decOrgCN;
    }

    public void setDecOrgCN(String decOrgCN) {
        this.decOrgCN = decOrgCN;
    }

    public String getRemExcpResCN() {
        return remExcpResCN;
    }

    public void setRemExcpResCN(String remExcpResCN) {
        this.remExcpResCN = remExcpResCN;
    }

    public String getRemDate() {
        return remDate;
    }

    public void setRemDate(String remDate) {
        this.remDate = remDate;
    }

    public String getReDecOrgCN() {
        return reDecOrgCN;
    }

    public void setReDecOrgCN(String reDecOrgCN) {
        this.reDecOrgCN = reDecOrgCN;
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
