package com.atguigu.kemai.entity.mango;
//
//            """处罚
//            "caseId": "fgwcf14000021194920",
//            "penDecNo": "广灵税壶泉简罚〔2019〕1000317号",
//            "illegActType": "违反税收管理",
//            "penContent": "2019-08-01至2019-08-31个人所得税（工资薪金所得）未按期进行申报",
//            "penAuth_CN": "国家税务总局大同市税务局",
//            "penDecIssDate": "2019-09-24",
//            "publicDate": "2019-09-24",
//            "name": "",
//            "regNo": "140223210000475",
//            "unitName": "",
//            "gtRegNo": "",
//            "leRep": "",
//            "alt": null,
//            "altDate": null,
//            "alt_penAuth_CN": null,
//            "type": "2",
//            "nodeNum": "140000",
//            "uniscId": "911402236781733190",
//            "detailUrl": "http://192.168.199.194:8082/gsxt/corp-query-entprise-new-allInfo-punishForPdf-fgwcf14000021194920.html?nodeNum=140000&entType=1&pripid=2140200000144878",
//            "fileName": "/doc/140000/casefiles/null",
//            "vPunishmentAltInfo": [],
//            "vPunishmentDecision": {
//        "desId": null,
//                "caseId": null,
//                "fileName": null,
//                "remark": null,
//                "lastModifiedTime": null
//    }
//    """处罚
public class EntPunishment {
    // # 案件id
    private String caseId;//
    // # 文件编号 行政处罚决定书文号 青市监罚字(2020)1036号
    private String decNo;//
    // # 违法类型/违法行为类型
    private String illegalType;//
    //# 违法内容/违法事实
    private String illegaContent;//
    //# 行政部门
    private String depCn;// # 处罚机关
    //# 签发日期
    private String issueDate;// # 处罚决定日期 2020-04-29
    //       # 公布日期
    private String publicDate;// # 处罚公式日期 2020-04-29， 没有就等于 处罚决定日期

    private String name;//
    private String regNo;// # 工商注册号
    private String unitName;//
    private String gtRegNo;//
    private String leRep;//
    private String alt;//
    private String altDate;//
    private String altDepCN;//
    private String fileName;//
    private String decision;// # 处罚內容
    private String nodeNum;// # 注册地编号 | 地方编码
    private String type;// #
    private String entName;// # 企业名称 | 行政相对人名称

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
        return "EntPunishment{" +
                "caseId='" + caseId + '\'' +
                ", decNo='" + decNo + '\'' +
                ", illegalType='" + illegalType + '\'' +
                ", illegaContent='" + illegaContent + '\'' +
                ", depCn='" + depCn + '\'' +
                ", issueDate='" + issueDate + '\'' +
                ", publicDate='" + publicDate + '\'' +
                ", name='" + name + '\'' +
                ", regNo='" + regNo + '\'' +
                ", unitName='" + unitName + '\'' +
                ", gtRegNo='" + gtRegNo + '\'' +
                ", leRep='" + leRep + '\'' +
                ", alt='" + alt + '\'' +
                ", altDate='" + altDate + '\'' +
                ", altDepCN='" + altDepCN + '\'' +
                ", fileName='" + fileName + '\'' +
                ", decision='" + decision + '\'' +
                ", nodeNum='" + nodeNum + '\'' +
                ", type='" + type + '\'' +
                ", entName='" + entName + '\'' +
                ", source='" + source + '\'' +
                ", sourceUrl='" + sourceUrl + '\'' +
                ", spider='" + spider + '\'' +
                '}';
    }

    public String getCaseId() {
        return caseId;
    }

    public void setCaseId(String caseId) {
        this.caseId = caseId;
    }

    public String getDecNo() {
        return decNo;
    }

    public void setDecNo(String decNo) {
        this.decNo = decNo;
    }

    public String getIllegalType() {
        return illegalType;
    }

    public void setIllegalType(String illegalType) {
        this.illegalType = illegalType;
    }

    public String getIllegaContent() {
        return illegaContent;
    }

    public void setIllegaContent(String illegaContent) {
        this.illegaContent = illegaContent;
    }

    public String getDepCn() {
        return depCn;
    }

    public void setDepCn(String depCn) {
        this.depCn = depCn;
    }

    public String getIssueDate() {
        return issueDate;
    }

    public void setIssueDate(String issueDate) {
        this.issueDate = issueDate;
    }

    public String getPublicDate() {
        return publicDate;
    }

    public void setPublicDate(String publicDate) {
        this.publicDate = publicDate;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRegNo() {
        return regNo;
    }

    public void setRegNo(String regNo) {
        this.regNo = regNo;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public String getGtRegNo() {
        return gtRegNo;
    }

    public void setGtRegNo(String gtRegNo) {
        this.gtRegNo = gtRegNo;
    }

    public String getLeRep() {
        return leRep;
    }

    public void setLeRep(String leRep) {
        this.leRep = leRep;
    }

    public String getAlt() {
        return alt;
    }

    public void setAlt(String alt) {
        this.alt = alt;
    }

    public String getAltDate() {
        return altDate;
    }

    public void setAltDate(String altDate) {
        this.altDate = altDate;
    }

    public String getAltDepCN() {
        return altDepCN;
    }

    public void setAltDepCN(String altDepCN) {
        this.altDepCN = altDepCN;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getDecision() {
        return decision;
    }

    public void setDecision(String decision) {
        this.decision = decision;
    }

    public String getNodeNum() {
        return nodeNum;
    }

    public void setNodeNum(String nodeNum) {
        this.nodeNum = nodeNum;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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
