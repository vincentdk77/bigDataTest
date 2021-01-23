package com.atguigu.kemai.entity.mango;


//股东
public class EntShareholder {
    private String perId; //
    private String invId; //
    private String inv; //# 持股人姓名
    private String invTypeCN; // # 股东类型：自然人股东、法人股东
    private String cerTypeCN; // # 证件类： 型中华人民共和国居民身份证
    private String cerNo; // # 证件号
    private String blicTypeCN; //
    private String liSubConAm; // 累计认缴额
    private String liAcConAm; // 实际认缴额
    private String countryCN; //
    private String dom; //
    private String respFormCN; //# 责任类型： 有限责任公司
    private String respForm; // # 责任类型
    private String sConForm; //
    private String sConFormCN; //
    private String conDate; //
    private String detailCheck; //
    private String url; //

    //       # 全站抓取时关联用
    private String entName; //

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
        return "EntShareholder{" +
                "perId='" + perId + '\'' +
                ", invId='" + invId + '\'' +
                ", inv='" + inv + '\'' +
                ", invTypeCN='" + invTypeCN + '\'' +
                ", cerTypeCN='" + cerTypeCN + '\'' +
                ", cerNo='" + cerNo + '\'' +
                ", blicTypeCN='" + blicTypeCN + '\'' +
                ", liSubConAm='" + liSubConAm + '\'' +
                ", liAcConAm='" + liAcConAm + '\'' +
                ", countryCN='" + countryCN + '\'' +
                ", dom='" + dom + '\'' +
                ", respFormCN='" + respFormCN + '\'' +
                ", respForm='" + respForm + '\'' +
                ", sConForm='" + sConForm + '\'' +
                ", sConFormCN='" + sConFormCN + '\'' +
                ", conDate='" + conDate + '\'' +
                ", detailCheck='" + detailCheck + '\'' +
                ", url='" + url + '\'' +
                ", entName='" + entName + '\'' +
                '}';
    }

    public String getPerId() {
        return perId;
    }

    public void setPerId(String perId) {
        this.perId = perId;
    }

    public String getInvId() {
        return invId;
    }

    public void setInvId(String invId) {
        this.invId = invId;
    }

    public String getInv() {
        return inv;
    }

    public void setInv(String inv) {
        this.inv = inv;
    }

    public String getInvTypeCN() {
        return invTypeCN;
    }

    public void setInvTypeCN(String invTypeCN) {
        this.invTypeCN = invTypeCN;
    }

    public String getCerTypeCN() {
        return cerTypeCN;
    }

    public void setCerTypeCN(String cerTypeCN) {
        this.cerTypeCN = cerTypeCN;
    }

    public String getCerNo() {
        return cerNo;
    }

    public void setCerNo(String cerNo) {
        this.cerNo = cerNo;
    }

    public String getBlicTypeCN() {
        return blicTypeCN;
    }

    public void setBlicTypeCN(String blicTypeCN) {
        this.blicTypeCN = blicTypeCN;
    }

    public String getLiSubConAm() {
        return liSubConAm;
    }

    public void setLiSubConAm(String liSubConAm) {
        this.liSubConAm = liSubConAm;
    }

    public String getLiAcConAm() {
        return liAcConAm;
    }

    public void setLiAcConAm(String liAcConAm) {
        this.liAcConAm = liAcConAm;
    }

    public String getCountryCN() {
        return countryCN;
    }

    public void setCountryCN(String countryCN) {
        this.countryCN = countryCN;
    }

    public String getDom() {
        return dom;
    }

    public void setDom(String dom) {
        this.dom = dom;
    }

    public String getRespFormCN() {
        return respFormCN;
    }

    public void setRespFormCN(String respFormCN) {
        this.respFormCN = respFormCN;
    }

    public String getRespForm() {
        return respForm;
    }

    public void setRespForm(String respForm) {
        this.respForm = respForm;
    }

    public String getsConForm() {
        return sConForm;
    }

    public void setsConForm(String sConForm) {
        this.sConForm = sConForm;
    }

    public String getsConFormCN() {
        return sConFormCN;
    }

    public void setsConFormCN(String sConFormCN) {
        this.sConFormCN = sConFormCN;
    }

    public String getConDate() {
        return conDate;
    }

    public void setConDate(String conDate) {
        this.conDate = conDate;
    }

    public String getDetailCheck() {
        return detailCheck;
    }

    public void setDetailCheck(String detailCheck) {
        this.detailCheck = detailCheck;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getEntName() {
        return entName;
    }

    public void setEntName(String entName) {
        this.entName = entName;
    }
}
