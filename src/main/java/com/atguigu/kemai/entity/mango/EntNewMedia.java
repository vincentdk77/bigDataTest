package com.atguigu.kemai.entity.mango;

public class EntNewMedia {
    //    """
//    新媒体：微信公众号 微博 头条 抖音 百度熊掌号 支付宝生活号 。。。
//            """
    private String type;//新媒体：1微信公众号 2微博 3头条 4抖音 5百度熊掌号 6支付宝生活号 。。。
    private String name;//
    private String accountName;//
    private String desc;//
    private String url;//

    private String source;//
    private String sourceUrl;//
    private String spider;//

    private String entName;//

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
        return "EntNewMedia{" +
                "type='" + type + '\'' +
                ", name='" + name + '\'' +
                ", accountName='" + accountName + '\'' +
                ", desc='" + desc + '\'' +
                ", url='" + url + '\'' +
                ", source='" + source + '\'' +
                ", sourceUrl='" + sourceUrl + '\'' +
                ", spider='" + spider + '\'' +
                ", entName='" + entName + '\'' +
                '}';
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAccountName() {
        return accountName;
    }

    public void setAccountName(String accountName) {
        this.accountName = accountName;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
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
