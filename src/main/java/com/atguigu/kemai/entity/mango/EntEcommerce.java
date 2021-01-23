package com.atguigu.kemai.entity.mango;

public class EntEcommerce {
    //电商店铺

    //# 商店评分
    private String shopRate;
    //# 商品类别
    private String itemCategories;
    //# 商量数量
    private String itemCount;
    //# 店铺名称
    private String shopName;
    //# 店铺url
    private String shopUrl;
    //# 数据来源站点名称
    private String source;
    //# 数据来源页面地址
    private String sourceUrl;
    //# 爬虫name
    private String spider;
    //# 全站抓取时关联用
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
        return "EntEcommerce{" +
                "shopRate='" + shopRate + '\'' +
                ", itemCategories='" + itemCategories + '\'' +
                ", itemCount='" + itemCount + '\'' +
                ", shopName='" + shopName + '\'' +
                ", shopUrl='" + shopUrl + '\'' +
                ", source='" + source + '\'' +
                ", sourceUrl='" + sourceUrl + '\'' +
                ", spider='" + spider + '\'' +
                ", entName='" + entName + '\'' +
                '}';
    }

    public String getShopRate() {
        return shopRate;
    }

    public void setShopRate(String shopRate) {
        this.shopRate = shopRate;
    }

    public String getItemCategories() {
        return itemCategories;
    }

    public void setItemCategories(String itemCategories) {
        this.itemCategories = itemCategories;
    }

    public String getItemCount() {
        return itemCount;
    }

    public void setItemCount(String itemCount) {
        this.itemCount = itemCount;
    }

    public String getShopName() {
        return shopName;
    }

    public void setShopName(String shopName) {
        this.shopName = shopName;
    }

    public String getShopUrl() {
        return shopUrl;
    }

    public void setShopUrl(String shopUrl) {
        this.shopUrl = shopUrl;
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
