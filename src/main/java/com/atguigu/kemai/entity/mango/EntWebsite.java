package com.atguigu.kemai.entity.mango;

public class EntWebsite {

    //首页布局
    private String  indexLayout;

    private String entName;
    private String domainTitle;

    // 官网IP信息:[{"domain": "", "regDate": "#域名注册日期", "ips": [{"ip":"ip", "loc": "位置",'idc':""}]}]

    private String ipList;

    //       # 关键词
    private String domainKeyword;
    //# 360收录量
    private String soIndex;
    //# 百度收录量
    private String baiduIndex;
    //# 360权重
    private String soWeight;
    // # 百度权重
    private String baiduWeight;
    // # ALEXA整站世界排名
    private String alexaRank;
    // # ALEXA整站流量排名
    private String alexaTrafficRank;

    //备案列表 {"domain": "", "name": "名称", "icp":"网站备案/许可证号", "apprDate": "审核时间"}

    private String icpList;

    //      # 网站模块:
    //      # 在线客服 0/1
    private String mCc;
    //      # 是否有注册登录 0/1
    private String mUser;
    //     # 是否有英文版 0/1
    private String mEnglish;
    //      # 是否有400电话： 400号码
    private String m400;
    // # 是否有案例 0/1
    private String mCase;
    //      # 是否有APP下载 0/1
    private String mApps;
    //      # 是否是 https： 0/1
    private String mHttps;
    //      # 是否有加入我们 0/1
    private String mJionus;
    //      # 是否有产品介绍 0/1
    private String mProducts;
    //      # 是否有招商 0/1
    private String mMerchants;
    //      # 是否有解决方案 0/1
    private String mSolution;

    //      # 技术参数: {"t": "", "w3c": "",  "errAvg": "平均错误率", "tts": "平均错误时间s"}
    private String techInfo;

    private String source;
    private String sourceUrl;
    private String spider;

    private String createTime;
    private String updateTime;

    private String domain;

    public String getIndexLayout() {
        return indexLayout;
    }

    public void setIndexLayout(String indexLayout) {
        this.indexLayout = indexLayout;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

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
        return "EntWebsite{" +
                "entName='" + entName + '\'' +
                ", domainTitle='" + domainTitle + '\'' +
                ", ipList='" + ipList + '\'' +
                ", domainKeyword='" + domainKeyword + '\'' +
                ", soIndex='" + soIndex + '\'' +
                ", baiduIndex='" + baiduIndex + '\'' +
                ", soWeight='" + soWeight + '\'' +
                ", baiduWeight='" + baiduWeight + '\'' +
                ", alexaRank='" + alexaRank + '\'' +
                ", alexaTrafficRank='" + alexaTrafficRank + '\'' +
                ", icpList='" + icpList + '\'' +
                ", mCc='" + mCc + '\'' +
                ", mUser='" + mUser + '\'' +
                ", mEnglish='" + mEnglish + '\'' +
                ", m400='" + m400 + '\'' +
                ", mCase='" + mCase + '\'' +
                ", mApps='" + mApps + '\'' +
                ", mHttps='" + mHttps + '\'' +
                ", mJionus='" + mJionus + '\'' +
                ", mProducts='" + mProducts + '\'' +
                ", mMerchants='" + mMerchants + '\'' +
                ", mSolution='" + mSolution + '\'' +
                ", techInfo='" + techInfo + '\'' +
                ", source='" + source + '\'' +
                ", sourceUrl='" + sourceUrl + '\'' +
                ", spider='" + spider + '\'' +
                '}';
    }

    public String getEntName() {
        return entName;
    }

    public void setEntName(String entName) {
        this.entName = entName;
    }

    public String getDomainTitle() {
        return domainTitle;
    }

    public void setDomainTitle(String domainTitle) {
        this.domainTitle = domainTitle;
    }

    public String getIpList() {
        return ipList;
    }

    public void setIpList(String ipList) {
        this.ipList = ipList;
    }

    public String getDomainKeyword() {
        return domainKeyword;
    }

    public void setDomainKeyword(String domainKeyword) {
        this.domainKeyword = domainKeyword;
    }

    public String getSoIndex() {
        return soIndex;
    }

    public void setSoIndex(String soIndex) {
        this.soIndex = soIndex;
    }

    public String getBaiduIndex() {
        return baiduIndex;
    }

    public void setBaiduIndex(String baiduIndex) {
        this.baiduIndex = baiduIndex;
    }

    public String getSoWeight() {
        return soWeight;
    }

    public void setSoWeight(String soWeight) {
        this.soWeight = soWeight;
    }

    public String getBaiduWeight() {
        return baiduWeight;
    }

    public void setBaiduWeight(String baiduWeight) {
        this.baiduWeight = baiduWeight;
    }

    public String getAlexaRank() {
        return alexaRank;
    }

    public void setAlexaRank(String alexaRank) {
        this.alexaRank = alexaRank;
    }

    public String getAlexaTrafficRank() {
        return alexaTrafficRank;
    }

    public void setAlexaTrafficRank(String alexaTrafficRank) {
        this.alexaTrafficRank = alexaTrafficRank;
    }

    public String getIcpList() {
        return icpList;
    }

    public void setIcpList(String icpList) {
        this.icpList = icpList;
    }

    public String getmCc() {
        return mCc;
    }

    public void setmCc(String mCc) {
        this.mCc = mCc;
    }

    public String getmUser() {
        return mUser;
    }

    public void setmUser(String mUser) {
        this.mUser = mUser;
    }

    public String getmEnglish() {
        return mEnglish;
    }

    public void setmEnglish(String mEnglish) {
        this.mEnglish = mEnglish;
    }

    public String getM400() {
        return m400;
    }

    public void setM400(String m400) {
        this.m400 = m400;
    }

    public String getmCase() {
        return mCase;
    }

    public void setmCase(String mCase) {
        this.mCase = mCase;
    }

    public String getmApps() {
        return mApps;
    }

    public void setmApps(String mApps) {
        this.mApps = mApps;
    }

    public String getmHttps() {
        return mHttps;
    }

    public void setmHttps(String mHttps) {
        this.mHttps = mHttps;
    }

    public String getmJionus() {
        return mJionus;
    }

    public void setmJionus(String mJionus) {
        this.mJionus = mJionus;
    }

    public String getmProducts() {
        return mProducts;
    }

    public void setmProducts(String mProducts) {
        this.mProducts = mProducts;
    }

    public String getmMerchants() {
        return mMerchants;
    }

    public void setmMerchants(String mMerchants) {
        this.mMerchants = mMerchants;
    }

    public String getmSolution() {
        return mSolution;
    }

    public void setmSolution(String mSolution) {
        this.mSolution = mSolution;
    }

    public String getTechInfo() {
        return techInfo;
    }

    public void setTechInfo(String techInfo) {
        this.techInfo = techInfo;
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
