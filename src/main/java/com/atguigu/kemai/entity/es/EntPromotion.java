package com.atguigu.kemai.entity.es;

public class EntPromotion {
    //     """推广
//             """
    private String entId;
    private String prmtText; //推广文案
    private String prmtSource;//推广渠道

    private String lastPrmtTime;//最后推广时间
    private String prmtKeyword;//推广关键词

    private String prmtKeyLM;//最近1个月推广关键词

    @Override
    public String toString() {
        return "EntPromotion{" +
                "entId='" + entId + '\'' +
                ", prmtText='" + prmtText + '\'' +
                ", prmtSource='" + prmtSource + '\'' +
                ", lastPrmtTime='" + lastPrmtTime + '\'' +
                ", prmtKeyword='" + prmtKeyword + '\'' +
                ", prmtKeyLM='" + prmtKeyLM + '\'' +
                '}';
    }

    public String getEntId() {
        return entId;
    }

    public void setEntId(String entId) {
        this.entId = entId;
    }

    public String getPrmtText() {
        return prmtText;
    }

    public void setPrmtText(String prmtText) {
        this.prmtText = prmtText;
    }

    public String getPrmtSource() {
        return prmtSource;
    }

    public void setPrmtSource(String prmtSource) {
        this.prmtSource = prmtSource;
    }

    public String getLastPrmtTime() {
        return lastPrmtTime;
    }

    public void setLastPrmtTime(String lastPrmtTime) {
        this.lastPrmtTime = lastPrmtTime;
    }

    public String getPrmtKeyword() {
        return prmtKeyword;
    }

    public void setPrmtKeyword(String prmtKeyword) {
        this.prmtKeyword = prmtKeyword;
    }

    public String getPrmtKeyLM() {
        return prmtKeyLM;
    }

    public void setPrmtKeyLM(String prmtKeyLM) {
        this.prmtKeyLM = prmtKeyLM;
    }
}
