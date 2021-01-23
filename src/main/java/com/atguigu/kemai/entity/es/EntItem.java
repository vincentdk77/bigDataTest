package com.atguigu.kemai.entity.es;

public class EntItem {

    private String entId; // #统一社会信用代码
    private String entName; //#企业名称
    private String desc; // #经营状态 （int）


    private String corpStatus; // #经营状态 （int）
    private String category; //#经营状态描述
    private String dom; // #企业类型: 数值类型
    private String opScope; //#企业类型字符
    private String postalAddr; // #企业类型中文描述：个体工商户 有限责任公司
    private String detailAddr; // #成立日期
    private String regCaption; // #核准日期

    private String regCap; // #
    private String regCapCurCN; //
    private String estDate; // #注册地
    private String entTypeCN; // #注册地编号
    private String licAnth; // #注册资本货币单位： 人民币


    private String licNameCN; // #注册资本数值: 100
    private String domain; // #注册资本大写 壹佰万
    private String legalName; // #工商注册号
    private String brandName; // #税号
    private String goodsName; // #

    /**
     * 雇员人数范围.
     */
    private String employeesMin;
    private String employeesMax;
    private String products; //# 主营产品

    private String regCity; //
    private String regProv; //
    private String regDistrict; //
    private String corpStatusString;




    //  # 统计字段

    private Integer branchCount; //分支机构
    private Integer licCount;//行政许可数量
    private Integer insuredCount;//参保人数
    private Integer contactCount;
    private Integer postCount;//当前招聘岗位数
    private Integer recruitCount;//当前招聘人数
    private Integer iCPCount;//ICP备案数量
    private Integer lTMPostCount;//近三个月招聘岗位数
    private Integer lTMJobCount;//近三个招聘总人数
    private Integer certCount;//证书数量
    private Integer weiboOrgCount;//机构微博数
    private Integer lMNewsCount;//近1月新闻数量
    private Integer prmtKeyCount;//推广关键词数
    private Integer prmtCompCount;//推广竞品数
    private Integer itemCount;//电商店铺商品总数
    private Integer goodsCount;//商品数量
    private Integer appCount;//应用数量
    private Integer brandCount;//品牌数量
    private Integer abnormalCount;//经营异常数量
    private Integer punishmentCount;//行政处罚数量
    private Integer tmValidCount;//有效状态商标数量
    private Integer tmInvalidCount;//无效状态商标数量
    private Integer patentCount;//专利数量
    private Integer lYPatentCount;//最近一年申请专利数量
    private Integer lYPatentPublicCount;//最近一年公开专利数量
    private Integer srCount;//软著数量
    private Integer crCount;//作品著作权数量
    private Integer wechatCount;//微信公众号
    private Integer financingCount;//融资次数
    private Integer appDownloadCount;//应用总下载量
    private Integer certThisYearCount;//本年度获证数量
    private Integer prmtLinksCount;//推广链接数
    private Integer ecShopCount;//电商店铺数量
    private Integer domainAgeCount;//域名年龄
    private Double appScoreAvg;//应用平均分
    private Integer salaryAvg;//岗位平均薪酬
    private String postUpdateTimeAvg;//岗位平均更新频率
    private Integer errorRateAvg;//平均访问错误率
    private Integer respTimeAvg;//平均访问速度
    private Integer isHighTech;//是否高新企业
    private Integer isTop500;//是否500强企业
    private Integer isUnicorn;//是否独角兽企业
    private Integer isGazelle;//是否瞪羚企业
    private Integer isStock;//是否上市企业
    private Integer isTaxARate;//是否A级纳税人
    private Integer isAbnormally;//当前是否被列入经营异常
    private Integer noLicence;//有无行政许可
    private Integer noInvest;//有无对外投资
    private Integer noAnnualInvest;//有无年报对外投资
    private Integer noLinkedin;//有无领英
    private Integer noAnnualReport;//有无企业年报
    private Integer noWeibo;//有无微博
    private Integer noFiscalAgent;//有无代理记账号码
    private Integer noMobile;//有无手机
    private Integer noTel;//有无固话
    private Integer noEmail;//有无邮箱
    private Integer noDomain;//有无官网
    private Integer noICP;//有无ICP备案
    private Integer noOnlineService;//有无在线客服
    private Integer noHttps;//有无Https模块
    private Integer noBidding;//有无招投标
    private Integer noTradeCredit;//有无进出口信用
    private Integer noCert;//有无证书
    private Integer noWeMedia;//有无自媒体
    private Integer noWeiboOrg;//有无微博机构号
    private Integer noNews;//有无新闻
    private Integer noWechat;//有无微信公众号
    private Integer noPrmt;//有无推广
    private Integer noGoods;//有无商品
    private Integer noApp;//有无应用
    private Integer noBrand;//有无品牌信息
    private Integer noAbnomalyOpt;//有无经营异常
    private Integer noPunishment;//有无行政处罚
    private Integer noEquityPledge;//有无股权出资
    private Integer noIllegal;//有无严重违法
    private Integer noMortgage;//有无动产抵押
    private Integer noIPRPledge;//有无知识产权出资
    private Integer noJudicialAid;//有无司法协助
    private Integer noJudicialPaper;//有无裁判文书
    private Integer noCourtNotice;//有无法院公告
    private Integer noTrademark;//有无商标
    private Integer noPatent;//有无专利
    private Integer noSr;//有无软著
    private Integer noCr;//有无作品著作权


    public String getCorpStatusString() {
        return corpStatusString;
    }

    public void setCorpStatusString(String corpStatusString) {
        this.corpStatusString = corpStatusString;
    }


    public String getRegCity() {
        return regCity;
    }

    public void setRegCity(String regCity) {
        this.regCity = regCity;
    }

    public String getRegProv() {
        return regProv;
    }

    public void setRegProv(String regProv) {
        this.regProv = regProv;
    }

    public String getRegDistrict() {
        return regDistrict;
    }

    public void setRegDistrict(String regDistrict) {
        this.regDistrict = regDistrict;
    }

    public String getProducts() {
        return products;
    }

    public void setProducts(String products) {
        this.products = products;
    }

    public String getEmployeesMin() {
        return employeesMin;
    }

    public void setEmployeesMin(String employeesMin) {
        this.employeesMin = employeesMin;
    }

    public String getEmployeesMax() {
        return employeesMax;
    }

    public void setEmployeesMax(String employeesMax) {
        this.employeesMax = employeesMax;
    }

    @Override
    public String toString() {
        return "EntItem{" +
                "entId='" + entId + '\'' +
                ", entName='" + entName + '\'' +
                ", entDesc='" + desc + '\'' +
                ", corpStatus='" + corpStatus + '\'' +
                ", category='" + category + '\'' +
                ", dom='" + dom + '\'' +
                ", opScope='" + opScope + '\'' +
                ", postalAddr='" + postalAddr + '\'' +
                ", detailAddr='" + detailAddr + '\'' +
                ", regCaption='" + regCaption + '\'' +
                ", regCap='" + regCap + '\'' +
                ", regCapCurCN='" + regCapCurCN + '\'' +
                ", estDate='" + estDate + '\'' +
                ", entTypeCN='" + entTypeCN + '\'' +
                ", licAnth='" + licAnth + '\'' +
                ", licNameCN='" + licNameCN + '\'' +
                ", domain='" + domain + '\'' +
                ", legalName='" + legalName + '\'' +
                ", brandName='" + brandName + '\'' +
                ", goodsName='" + goodsName + '\'' +
                ", branchCount=" + branchCount +
                ", licCount=" + licCount +
                ", insuredCount=" + insuredCount +
                ", contactCount=" + contactCount +
                ", postCount=" + postCount +
                ", recruitCount=" + recruitCount +
                ", iCPCount=" + iCPCount +
                ", lTMPostCount=" + lTMPostCount +
                ", lTMJobCount=" + lTMJobCount +
                ", certCount=" + certCount +
                ", weiboOrgCount=" + weiboOrgCount +
                ", lMNewsCount=" + lMNewsCount +
                ", prmtKeyCount=" + prmtKeyCount +
                ", prmtCompCount=" + prmtCompCount +
                ", itemCount=" + itemCount +
                ", goodsCount=" + goodsCount +
                ", appCount=" + appCount +
                ", brandCount=" + brandCount +
                ", abnormalCount=" + abnormalCount +
                ", punishmentCount=" + punishmentCount +
                ", tmValidCount=" + tmValidCount +
                ", tmInvalidCount=" + tmInvalidCount +
                ", patentCount=" + patentCount +
                ", lYPatentCount=" + lYPatentCount +
                ", lYPatentPublicCount=" + lYPatentPublicCount +
                ", srCount=" + srCount +
                ", crCount=" + crCount +
                ", wechatCount=" + wechatCount +
                ", financingCount=" + financingCount +
                ", appDownloadCount=" + appDownloadCount +
                ", certThisYearCount=" + certThisYearCount +
                ", prmtLinksCount=" + prmtLinksCount +
                ", ecShopCount=" + ecShopCount +
                ", domainAgeCount=" + domainAgeCount +
                ", appScoreAvg=" + appScoreAvg +
                ", salaryAvg=" + salaryAvg +
                ", postUpdateTimeAvg='" + postUpdateTimeAvg + '\'' +
                ", errorRateAvg=" + errorRateAvg +
                ", respTimeAvg=" + respTimeAvg +
                ", isHighTech=" + isHighTech +
                ", isTop500=" + isTop500 +
                ", isUnicorn=" + isUnicorn +
                ", isGazelle=" + isGazelle +
                ", isStock=" + isStock +
                ", isTaxARate=" + isTaxARate +
                ", isAbnormally=" + isAbnormally +
                ", noLicence=" + noLicence +
                ", noInvest=" + noInvest +
                ", noAnnualInvest=" + noAnnualInvest +
                ", noLinkedin=" + noLinkedin +
                ", noAnnualReport=" + noAnnualReport +
                ", noWeibo=" + noWeibo +
                ", noFiscalAgent=" + noFiscalAgent +
                ", noMobile=" + noMobile +
                ", noTel=" + noTel +
                ", noEmail=" + noEmail +
                ", noDomain=" + noDomain +
                ", noICP=" + noICP +
                ", noOnlineService=" + noOnlineService +
                ", noHttps=" + noHttps +
                ", noBidding=" + noBidding +
                ", noTradeCredit=" + noTradeCredit +
                ", noCert=" + noCert +
                ", noWeMedia=" + noWeMedia +
                ", noWeiboOrg=" + noWeiboOrg +
                ", noNews=" + noNews +
                ", noWechat=" + noWechat +
                ", noPrmt=" + noPrmt +
                ", noGoods=" + noGoods +
                ", noApp=" + noApp +
                ", noBrand=" + noBrand +
                ", noAbnomalyOpt=" + noAbnomalyOpt +
                ", noPunishment=" + noPunishment +
                ", noEquityPledge=" + noEquityPledge +
                ", noIllegal=" + noIllegal +
                ", noMortgage=" + noMortgage +
                ", noIPRPledge=" + noIPRPledge +
                ", noJudicialAid=" + noJudicialAid +
                ", noJudicialPaper=" + noJudicialPaper +
                ", noCourtNotice=" + noCourtNotice +
                ", noTrademark=" + noTrademark +
                ", noPatent=" + noPatent +
                ", noSr=" + noSr +
                ", noCr=" + noCr +
                '}';
    }

    public String getEntId() {
        return entId;
    }

    public void setEntId(String entId) {
        this.entId = entId;
    }

    public String getEntName() {
        return entName;
    }

    public void setEntName(String entName) {
        this.entName = entName;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String entDesc) {
        this.desc = entDesc;
    }

    public String getCorpStatus() {
        return corpStatus;
    }

    public void setCorpStatus(String corpStatus) {
        this.corpStatus = corpStatus;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getDom() {
        return dom;
    }

    public void setDom(String dom) {
        this.dom = dom;
    }

    public String getOpScope() {
        return opScope;
    }

    public void setOpScope(String opScope) {
        this.opScope = opScope;
    }

    public String getPostalAddr() {
        return postalAddr;
    }

    public void setPostalAddr(String postalAddr) {
        this.postalAddr = postalAddr;
    }

    public String getDetailAddr() {
        return detailAddr;
    }

    public void setDetailAddr(String detailAddr) {
        this.detailAddr = detailAddr;
    }

    public String getRegCaption() {
        return regCaption;
    }

    public void setRegCaption(String regCaption) {
        this.regCaption = regCaption;
    }

    public String getRegCap() {
        return regCap;
    }

    public void setRegCap(String regCap) {
        this.regCap = regCap;
    }

    public String getRegCapCurCN() {
        return regCapCurCN;
    }

    public void setRegCapCurCN(String regCapCurCN) {
        this.regCapCurCN = regCapCurCN;
    }

    public String getEstDate() {
        return estDate;
    }

    public void setEstDate(String estDate) {
        this.estDate = estDate;
    }

    public String getEntTypeCN() {
        return entTypeCN;
    }

    public void setEntTypeCN(String entTypeCN) {
        this.entTypeCN = entTypeCN;
    }

    public String getLicAnth() {
        return licAnth;
    }

    public void setLicAnth(String licAnth) {
        this.licAnth = licAnth;
    }

    public String getLicNameCN() {
        return licNameCN;
    }

    public void setLicNameCN(String licNameCN) {
        this.licNameCN = licNameCN;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getLegalName() {
        return legalName;
    }

    public void setLegalName(String legalName) {
        this.legalName = legalName;
    }

    public String getBrandName() {
        return brandName;
    }

    public void setBrandName(String brandName) {
        this.brandName = brandName;
    }

    public String getGoodsName() {
        return goodsName;
    }

    public void setGoodsName(String goodsName) {
        this.goodsName = goodsName;
    }

    public Integer getBranchCount() {
        return branchCount;
    }

    public void setBranchCount(Integer branchCount) {
        this.branchCount = branchCount;
    }

    public Integer getLicCount() {
        return licCount;
    }

    public void setLicCount(Integer licCount) {
        this.licCount = licCount;
    }

    public Integer getInsuredCount() {
        return insuredCount;
    }

    public void setInsuredCount(Integer insuredCount) {
        this.insuredCount = insuredCount;
    }

    public Integer getContactCount() {
        return contactCount;
    }

    public void setContactCount(Integer contactCount) {
        this.contactCount = contactCount;
    }

    public Integer getPostCount() {
        return postCount;
    }

    public void setPostCount(Integer postCount) {
        this.postCount = postCount;
    }

    public Integer getRecruitCount() {
        return recruitCount;
    }

    public void setRecruitCount(Integer recruitCount) {
        this.recruitCount = recruitCount;
    }

    public Integer getiCPCount() {
        return iCPCount;
    }

    public void setiCPCount(Integer iCPCount) {
        this.iCPCount = iCPCount;
    }

    public Integer getlTMPostCount() {
        return lTMPostCount;
    }

    public void setlTMPostCount(Integer lTMPostCount) {
        this.lTMPostCount = lTMPostCount;
    }

    public Integer getlTMJobCount() {
        return lTMJobCount;
    }

    public void setlTMJobCount(Integer lTMJobCount) {
        this.lTMJobCount = lTMJobCount;
    }

    public Integer getCertCount() {
        return certCount;
    }

    public void setCertCount(Integer certCount) {
        this.certCount = certCount;
    }

    public Integer getWeiboOrgCount() {
        return weiboOrgCount;
    }

    public void setWeiboOrgCount(Integer weiboOrgCount) {
        this.weiboOrgCount = weiboOrgCount;
    }

    public Integer getlMNewsCount() {
        return lMNewsCount;
    }

    public void setlMNewsCount(Integer lMNewsCount) {
        this.lMNewsCount = lMNewsCount;
    }

    public Integer getPrmtKeyCount() {
        return prmtKeyCount;
    }

    public void setPrmtKeyCount(Integer prmtKeyCount) {
        this.prmtKeyCount = prmtKeyCount;
    }

    public Integer getPrmtCompCount() {
        return prmtCompCount;
    }

    public void setPrmtCompCount(Integer prmtCompCount) {
        this.prmtCompCount = prmtCompCount;
    }

    public Integer getItemCount() {
        return itemCount;
    }

    public void setItemCount(Integer itemCount) {
        this.itemCount = itemCount;
    }

    public Integer getGoodsCount() {
        return goodsCount;
    }

    public void setGoodsCount(Integer goodsCount) {
        this.goodsCount = goodsCount;
    }

    public Integer getAppCount() {
        return appCount;
    }

    public void setAppCount(Integer appCount) {
        this.appCount = appCount;
    }

    public Integer getBrandCount() {
        return brandCount;
    }

    public void setBrandCount(Integer brandCount) {
        this.brandCount = brandCount;
    }

    public Integer getAbnormalCount() {
        return abnormalCount;
    }

    public void setAbnormalCount(Integer abnormalCount) {
        this.abnormalCount = abnormalCount;
    }

    public Integer getPunishmentCount() {
        return punishmentCount;
    }

    public void setPunishmentCount(Integer punishmentCount) {
        this.punishmentCount = punishmentCount;
    }

    public Integer getTmValidCount() {
        return tmValidCount;
    }

    public void setTmValidCount(Integer tmValidCount) {
        this.tmValidCount = tmValidCount;
    }

    public Integer getTmInvalidCount() {
        return tmInvalidCount;
    }

    public void setTmInvalidCount(Integer tmInvalidCount) {
        this.tmInvalidCount = tmInvalidCount;
    }

    public Integer getPatentCount() {
        return patentCount;
    }

    public void setPatentCount(Integer patentCount) {
        this.patentCount = patentCount;
    }

    public Integer getlYPatentCount() {
        return lYPatentCount;
    }

    public void setlYPatentCount(Integer lYPatentCount) {
        this.lYPatentCount = lYPatentCount;
    }

    public Integer getlYPatentPublicCount() {
        return lYPatentPublicCount;
    }

    public void setlYPatentPublicCount(Integer lYPatentPublicCount) {
        this.lYPatentPublicCount = lYPatentPublicCount;
    }

    public Integer getSrCount() {
        return srCount;
    }

    public void setSrCount(Integer srCount) {
        this.srCount = srCount;
    }

    public Integer getCrCount() {
        return crCount;
    }

    public void setCrCount(Integer crCount) {
        this.crCount = crCount;
    }

    public Integer getWechatCount() {
        return wechatCount;
    }

    public void setWechatCount(Integer wechatCount) {
        this.wechatCount = wechatCount;
    }

    public Integer getFinancingCount() {
        return financingCount;
    }

    public void setFinancingCount(Integer financingCount) {
        this.financingCount = financingCount;
    }

    public Integer getAppDownloadCount() {
        return appDownloadCount;
    }

    public void setAppDownloadCount(Integer appDownloadCount) {
        this.appDownloadCount = appDownloadCount;
    }

    public Integer getCertThisYearCount() {
        return certThisYearCount;
    }

    public void setCertThisYearCount(Integer certThisYearCount) {
        this.certThisYearCount = certThisYearCount;
    }

    public Integer getPrmtLinksCount() {
        return prmtLinksCount;
    }

    public void setPrmtLinksCount(Integer prmtLinksCount) {
        this.prmtLinksCount = prmtLinksCount;
    }

    public Integer getEcShopCount() {
        return ecShopCount;
    }

    public void setEcShopCount(Integer ecShopCount) {
        this.ecShopCount = ecShopCount;
    }

    public Integer getDomainAgeCount() {
        return domainAgeCount;
    }

    public void setDomainAgeCount(Integer domainAgeCount) {
        this.domainAgeCount = domainAgeCount;
    }

    public Double getAppScoreAvg() {
        return appScoreAvg;
    }

    public void setAppScoreAvg(Double appScoreAvg) {
        this.appScoreAvg = appScoreAvg;
    }

    public Integer getSalaryAvg() {
        return salaryAvg;
    }

    public void setSalaryAvg(Integer salaryAvg) {
        this.salaryAvg = salaryAvg;
    }

    public String getPostUpdateTimeAvg() {
        return postUpdateTimeAvg;
    }

    public void setPostUpdateTimeAvg(String postUpdateTimeAvg) {
        this.postUpdateTimeAvg = postUpdateTimeAvg;
    }

    public Integer getErrorRateAvg() {
        return errorRateAvg;
    }

    public void setErrorRateAvg(Integer errorRateAvg) {
        this.errorRateAvg = errorRateAvg;
    }

    public Integer getRespTimeAvg() {
        return respTimeAvg;
    }

    public void setRespTimeAvg(Integer respTimeAvg) {
        this.respTimeAvg = respTimeAvg;
    }

    public Integer getIsHighTech() {
        return isHighTech;
    }

    public void setIsHighTech(Integer isHighTech) {
        this.isHighTech = isHighTech;
    }

    public Integer getIsTop500() {
        return isTop500;
    }

    public void setIsTop500(Integer isTop500) {
        this.isTop500 = isTop500;
    }

    public Integer getIsUnicorn() {
        return isUnicorn;
    }

    public void setIsUnicorn(Integer isUnicorn) {
        this.isUnicorn = isUnicorn;
    }

    public Integer getIsGazelle() {
        return isGazelle;
    }

    public void setIsGazelle(Integer isGazelle) {
        this.isGazelle = isGazelle;
    }

    public Integer getIsStock() {
        return isStock;
    }

    public void setIsStock(Integer isStock) {
        this.isStock = isStock;
    }

    public Integer getIsTaxARate() {
        return isTaxARate;
    }

    public void setIsTaxARate(Integer isTaxARate) {
        this.isTaxARate = isTaxARate;
    }

    public Integer getIsAbnormally() {
        return isAbnormally;
    }

    public void setIsAbnormally(Integer isAbnormally) {
        this.isAbnormally = isAbnormally;
    }

    public Integer getNoLicence() {
        return noLicence;
    }

    public void setNoLicence(Integer noLicence) {
        this.noLicence = noLicence;
    }

    public Integer getNoInvest() {
        return noInvest;
    }

    public void setNoInvest(Integer noInvest) {
        this.noInvest = noInvest;
    }

    public Integer getNoAnnualInvest() {
        return noAnnualInvest;
    }

    public void setNoAnnualInvest(Integer noAnnualInvest) {
        this.noAnnualInvest = noAnnualInvest;
    }

    public Integer getNoLinkedin() {
        return noLinkedin;
    }

    public void setNoLinkedin(Integer noLinkedin) {
        this.noLinkedin = noLinkedin;
    }

    public Integer getNoAnnualReport() {
        return noAnnualReport;
    }

    public void setNoAnnualReport(Integer noAnnualReport) {
        this.noAnnualReport = noAnnualReport;
    }

    public Integer getNoWeibo() {
        return noWeibo;
    }

    public void setNoWeibo(Integer noWeibo) {
        this.noWeibo = noWeibo;
    }

    public Integer getNoFiscalAgent() {
        return noFiscalAgent;
    }

    public void setNoFiscalAgent(Integer noFiscalAgent) {
        this.noFiscalAgent = noFiscalAgent;
    }

    public Integer getNoMobile() {
        return noMobile;
    }

    public void setNoMobile(Integer noMobile) {
        this.noMobile = noMobile;
    }

    public Integer getNoTel() {
        return noTel;
    }

    public void setNoTel(Integer noTel) {
        this.noTel = noTel;
    }

    public Integer getNoEmail() {
        return noEmail;
    }

    public void setNoEmail(Integer noEmail) {
        this.noEmail = noEmail;
    }

    public Integer getNoDomain() {
        return noDomain;
    }

    public void setNoDomain(Integer noDomain) {
        this.noDomain = noDomain;
    }

    public Integer getNoICP() {
        return noICP;
    }

    public void setNoICP(Integer noICP) {
        this.noICP = noICP;
    }

    public Integer getNoOnlineService() {
        return noOnlineService;
    }

    public void setNoOnlineService(Integer noOnlineService) {
        this.noOnlineService = noOnlineService;
    }

    public Integer getNoHttps() {
        return noHttps;
    }

    public void setNoHttps(Integer noHttps) {
        this.noHttps = noHttps;
    }

    public Integer getNoBidding() {
        return noBidding;
    }

    public void setNoBidding(Integer noBidding) {
        this.noBidding = noBidding;
    }

    public Integer getNoTradeCredit() {
        return noTradeCredit;
    }

    public void setNoTradeCredit(Integer noTradeCredit) {
        this.noTradeCredit = noTradeCredit;
    }

    public Integer getNoCert() {
        return noCert;
    }

    public void setNoCert(Integer noCert) {
        this.noCert = noCert;
    }

    public Integer getNoWeMedia() {
        return noWeMedia;
    }

    public void setNoWeMedia(Integer noWeMedia) {
        this.noWeMedia = noWeMedia;
    }

    public Integer getNoWeiboOrg() {
        return noWeiboOrg;
    }

    public void setNoWeiboOrg(Integer noWeiboOrg) {
        this.noWeiboOrg = noWeiboOrg;
    }

    public Integer getNoNews() {
        return noNews;
    }

    public void setNoNews(Integer noNews) {
        this.noNews = noNews;
    }

    public Integer getNoWechat() {
        return noWechat;
    }

    public void setNoWechat(Integer noWechat) {
        this.noWechat = noWechat;
    }

    public Integer getNoPrmt() {
        return noPrmt;
    }

    public void setNoPrmt(Integer noPrmt) {
        this.noPrmt = noPrmt;
    }

    public Integer getNoGoods() {
        return noGoods;
    }

    public void setNoGoods(Integer noGoods) {
        this.noGoods = noGoods;
    }

    public Integer getNoApp() {
        return noApp;
    }

    public void setNoApp(Integer noApp) {
        this.noApp = noApp;
    }

    public Integer getNoBrand() {
        return noBrand;
    }

    public void setNoBrand(Integer noBrand) {
        this.noBrand = noBrand;
    }

    public Integer getNoAbnomalyOpt() {
        return noAbnomalyOpt;
    }

    public void setNoAbnomalyOpt(Integer noAbnomalyOpt) {
        this.noAbnomalyOpt = noAbnomalyOpt;
    }

    public Integer getNoPunishment() {
        return noPunishment;
    }

    public void setNoPunishment(Integer noPunishment) {
        this.noPunishment = noPunishment;
    }

    public Integer getNoEquityPledge() {
        return noEquityPledge;
    }

    public void setNoEquityPledge(Integer noEquityPledge) {
        this.noEquityPledge = noEquityPledge;
    }

    public Integer getNoIllegal() {
        return noIllegal;
    }

    public void setNoIllegal(Integer noIllegal) {
        this.noIllegal = noIllegal;
    }

    public Integer getNoMortgage() {
        return noMortgage;
    }

    public void setNoMortgage(Integer noMortgage) {
        this.noMortgage = noMortgage;
    }

    public Integer getNoIPRPledge() {
        return noIPRPledge;
    }

    public void setNoIPRPledge(Integer noIPRPledge) {
        this.noIPRPledge = noIPRPledge;
    }

    public Integer getNoJudicialAid() {
        return noJudicialAid;
    }

    public void setNoJudicialAid(Integer noJudicialAid) {
        this.noJudicialAid = noJudicialAid;
    }

    public Integer getNoJudicialPaper() {
        return noJudicialPaper;
    }

    public void setNoJudicialPaper(Integer noJudicialPaper) {
        this.noJudicialPaper = noJudicialPaper;
    }

    public Integer getNoCourtNotice() {
        return noCourtNotice;
    }

    public void setNoCourtNotice(Integer noCourtNotice) {
        this.noCourtNotice = noCourtNotice;
    }

    public Integer getNoTrademark() {
        return noTrademark;
    }

    public void setNoTrademark(Integer noTrademark) {
        this.noTrademark = noTrademark;
    }

    public Integer getNoPatent() {
        return noPatent;
    }

    public void setNoPatent(Integer noPatent) {
        this.noPatent = noPatent;
    }

    public Integer getNoSr() {
        return noSr;
    }

    public void setNoSr(Integer noSr) {
        this.noSr = noSr;
    }

    public Integer getNoCr() {
        return noCr;
    }

    public void setNoCr(Integer noCr) {
        this.noCr = noCr;
    }
}
