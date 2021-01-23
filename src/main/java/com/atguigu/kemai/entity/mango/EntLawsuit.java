package com.atguigu.kemai.entity.mango;

public class EntLawsuit {
    //    """
//    诉讼
//    """
    private String caseName;//  # 案件名称
    private String caseNo;//# 案件号
    private String date;// # 立案时间
    private String pubDate;//# 公告日期 | 发布日期
    private String pubDep;// # 公告部门
    private String caseDesc;//# 案件/公告内容/生效法律文书确定的义务
    private String pubPage;// # 刊登版面
    private String typeCN;// # 裁判文书 | 法院公告 | 起诉状、上诉状副本 | 被执行

    private String execNo;// # 执行依据文号
    private String execCurStatus;// # 执行履行情况
    private String execStatus;// # 失信被执行人行为具体情形

    private String plaintiff;// # 原告
    private String defendant;// # 被告

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
        return "EntLawsuit{" +
                "caseName='" + caseName + '\'' +
                ", caseNo='" + caseNo + '\'' +
                ", date='" + date + '\'' +
                ", pubDate='" + pubDate + '\'' +
                ", pubDep='" + pubDep + '\'' +
                ", caseDesc='" + caseDesc + '\'' +
                ", pubPage='" + pubPage + '\'' +
                ", typeCN='" + typeCN + '\'' +
                ", execNo='" + execNo + '\'' +
                ", execCurStatus='" + execCurStatus + '\'' +
                ", execStatus='" + execStatus + '\'' +
                ", plaintiff='" + plaintiff + '\'' +
                ", defendant='" + defendant + '\'' +
                ", entName='" + entName + '\'' +
                ", source='" + source + '\'' +
                ", sourceUrl='" + sourceUrl + '\'' +
                ", spider='" + spider + '\'' +
                '}';
    }

    public String getCaseName() {
        return caseName;
    }

    public void setCaseName(String caseName) {
        this.caseName = caseName;
    }

    public String getCaseNo() {
        return caseNo;
    }

    public void setCaseNo(String caseNo) {
        this.caseNo = caseNo;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getPubDate() {
        return pubDate;
    }

    public void setPubDate(String pubDate) {
        this.pubDate = pubDate;
    }

    public String getPubDep() {
        return pubDep;
    }

    public void setPubDep(String pubDep) {
        this.pubDep = pubDep;
    }

    public String getCaseDesc() {
        return caseDesc;
    }

    public void setCaseDesc(String caseDesc) {
        this.caseDesc = caseDesc;
    }

    public String getPubPage() {
        return pubPage;
    }

    public void setPubPage(String pubPage) {
        this.pubPage = pubPage;
    }

    public String getTypeCN() {
        return typeCN;
    }

    public void setTypeCN(String typeCN) {
        this.typeCN = typeCN;
    }

    public String getExecNo() {
        return execNo;
    }

    public void setExecNo(String execNo) {
        this.execNo = execNo;
    }

    public String getExecCurStatus() {
        return execCurStatus;
    }

    public void setExecCurStatus(String execCurStatus) {
        this.execCurStatus = execCurStatus;
    }

    public String getExecStatus() {
        return execStatus;
    }

    public void setExecStatus(String execStatus) {
        this.execStatus = execStatus;
    }

    public String getPlaintiff() {
        return plaintiff;
    }

    public void setPlaintiff(String plaintiff) {
        this.plaintiff = plaintiff;
    }

    public String getDefendant() {
        return defendant;
    }

    public void setDefendant(String defendant) {
        this.defendant = defendant;
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
