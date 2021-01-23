package com.atguigu.kemai.entity.mango;

//变动
public class EntAlterinfo {
    private String uniscId;
    private String altItemCN;
    private String altBe;
    private String altAf;
    private String altDate;
    //      # 全站抓取时关联用
    private String entName;

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
        return "EntAlterinfo{" +
                "uniscId='" + uniscId + '\'' +
                ", altItemCN='" + altItemCN + '\'' +
                ", altBe='" + altBe + '\'' +
                ", altAf='" + altAf + '\'' +
                ", altDate='" + altDate + '\'' +
                ", entName='" + entName + '\'' +
                '}';
    }



    public String getUniscId() {
        return uniscId;
    }

    public void setUniscId(String uniscId) {
        this.uniscId = uniscId;
    }

    public String getAltItemCN() {
        return altItemCN;
    }

    public void setAltItemCN(String altItemCN) {
        this.altItemCN = altItemCN;
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

    public String getEntName() {
        return entName;
    }

    public void setEntName(String entName) {
        this.entName = entName;
    }
}
