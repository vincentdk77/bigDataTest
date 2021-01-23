package com.atguigu.kemai.entity.es;

public class EntPatent {
    private String entId;
    private String tmName;//商标名称
    private String propertyEndDate;//商标专用权结束日期
    private String patentCate;//涵盖专利类比
    private String patentName;//专利名称
    private String softName;//软件名称
    private String crName;//作品名称

    @Override
    public String toString() {
        return "EntPatent{" +
                "entId='" + entId + '\'' +
                ", tmName='" + tmName + '\'' +
                ", propertyEndDate='" + propertyEndDate + '\'' +
                ", patentCate='" + patentCate + '\'' +
                ", patentName='" + patentName + '\'' +
                ", softName='" + softName + '\'' +
                ", crName='" + crName + '\'' +
                '}';
    }

    public String getEntId() {
        return entId;
    }

    public void setEntId(String entId) {
        this.entId = entId;
    }

    public String getTmName() {
        return tmName;
    }

    public void setTmName(String tmName) {
        this.tmName = tmName;
    }

    public String getPropertyEndDate() {
        return propertyEndDate;
    }

    public void setPropertyEndDate(String propertyEndDate) {
        this.propertyEndDate = propertyEndDate;
    }

    public String getPatentCate() {
        return patentCate;
    }

    public void setPatentCate(String patentCate) {
        this.patentCate = patentCate;
    }

    public String getPatentName() {
        return patentName;
    }

    public void setPatentName(String patentName) {
        this.patentName = patentName;
    }

    public String getSoftName() {
        return softName;
    }

    public void setSoftName(String softName) {
        this.softName = softName;
    }

    public String getCrName() {
        return crName;
    }

    public void setCrName(String crName) {
        this.crName = crName;
    }
}
