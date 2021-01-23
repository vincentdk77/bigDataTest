package com.atguigu.kemai.entity.mango;

public class EntContacts {

//    TYPE_MOBILE = 1
//    TYPE_TEL = 2
//    TYPE_EMAIL = 3
//    TYPE_QQ = 4
//    TYPE_WX = 5
//    TYPE_FOX = 6
    //联系方式

    private String uniscId;
    //    # 联系人姓名
    private String name;
    //# 号码、邮箱、QQ...内容
    private String content;
    //# 1手机， 2固话， 3邮箱，4QQ, 5微信, 6传真
    private String type;
    //# 如果目标站点有更新时间数据的话
    private String updateTime;
    //# 数据来源站点名称
    private String source;
    // # 来源页面地址
    private String sourceUrl;
    //# 爬虫name
    private String spider;
    // # 全站抓取时关联用
    private String entName;

    private String createTime;

    //手机号所在地
    private String region;
    //手机号所属运营商
    private String operator;


    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }


    @Override
    public String toString() {
        return "EntContacts{" +
                "uniscId='" + uniscId + '\'' +
                ", name='" + name + '\'' +
                ", content='" + content + '\'' +
                ", type='" + type + '\'' +
                ", updateTime='" + updateTime + '\'' +
                ", source='" + source + '\'' +
                ", sourceUrl='" + sourceUrl + '\'' +
                ", spider='" + spider + '\'' +
                ", entName='" + entName + '\'' +
                '}';
    }

    public String getUniscId() {
        return uniscId;
    }

    public void setUniscId(String uniscId) {
        this.uniscId = uniscId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
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

    public String getEntName() {
        return entName;
    }

    public void setEntName(String entName) {
        this.entName = entName;
    }
}
