package com.atguigu.kemai.entity.mango;


public class EntKeyPerson {
    private String perId;
    private String pripId;
    private String name;
    private String positionCN;
    //       # 全站抓取时关联用
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
        return "EntKeyPerson{" +
                "perId='" + perId + '\'' +
                ", pripId='" + pripId + '\'' +
                ", name='" + name + '\'' +
                ", positionCN='" + positionCN + '\'' +
                ", entName='" + entName + '\'' +
                '}';
    }

    public String getPerId() {
        return perId;
    }

    public void setPerId(String perId) {
        this.perId = perId;
    }

    public String getPripId() {
        return pripId;
    }

    public void setPripId(String pripId) {
        this.pripId = pripId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPositionCN() {
        return positionCN;
    }

    public void setPositionCN(String positionCN) {
        this.positionCN = positionCN;
    }

    public String getEntName() {
        return entName;
    }

    public void setEntName(String entName) {
        this.entName = entName;
    }
}
