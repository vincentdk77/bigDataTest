package com.atguigu.kemai.entity.es;

public class EntAbnormalOpt {
    //    """经营异常
//            """
    private String entId;//
    // # 异常内容
    private String speCauseCN;//
    // # 异常时间
    private String abntime;//
    //# 移除日期
    private String remDate;//


    private String lastPunishmentDate;//

    private String illegalType;//

    private String legalCaseCause;//
    private String courtNoticeType;//

    @Override
    public String toString() {
        return "EntAbnormalOpt{" +
                "entId='" + entId + '\'' +
                ", speCauseCN='" + speCauseCN + '\'' +
                ", abntime='" + abntime + '\'' +
                ", remDate='" + remDate + '\'' +
                ", lastPunishmentDate='" + lastPunishmentDate + '\'' +
                ", illegalType='" + illegalType + '\'' +
                ", legalCaseCause='" + legalCaseCause + '\'' +
                ", courtNoticeType='" + courtNoticeType + '\'' +
                '}';
    }

    public String getEntId() {
        return entId;
    }

    public void setEntId(String entId) {
        this.entId = entId;
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

    public String getRemDate() {
        return remDate;
    }

    public void setRemDate(String remDate) {
        this.remDate = remDate;
    }

    public String getLastPunishmentDate() {
        return lastPunishmentDate;
    }

    public void setLastPunishmentDate(String lastPunishmentDate) {
        this.lastPunishmentDate = lastPunishmentDate;
    }

    public String getIllegalType() {
        return illegalType;
    }

    public void setIllegalType(String illegalType) {
        this.illegalType = illegalType;
    }

    public String getLegalCaseCause() {
        return legalCaseCause;
    }

    public void setLegalCaseCause(String legalCaseCause) {
        this.legalCaseCause = legalCaseCause;
    }

    public String getCourtNoticeType() {
        return courtNoticeType;
    }

    public void setCourtNoticeType(String courtNoticeType) {
        this.courtNoticeType = courtNoticeType;
    }
}
