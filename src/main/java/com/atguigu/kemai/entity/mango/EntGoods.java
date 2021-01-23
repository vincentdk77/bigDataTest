package com.atguigu.kemai.entity.mango;

public class EntGoods {
    //  生产或供应

    //        # 货物图片地址
    private String uniscId;
    private String entName;
    private String goodsNameImgUrl;
    //        # 货物名称
    private String goodsName;
    //  # 型号 类型 代码....
    private String goodsCode;
    //         # 单位
    private String goodsUnit;
    //  # 价格
    private String price;
    // # 条形码
    private String barCode;

    private String pubTime;
    //  # 数据来源站点名称
    private String source;
    //  # 数据来源页面地址
    private String sourceUrl;
    //  # 爬虫name
    private String spider;

    private String createTime;
    private String updateTime;

    public String getPubTime() {
        return pubTime;
    }

    public void setPubTime(String pubTime) {
        this.pubTime = pubTime;
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
        return "EntGoods{" +
                "uniscId='" + uniscId + '\'' +
                ", entName='" + entName + '\'' +
                ", goodsNameImgUrl='" + goodsNameImgUrl + '\'' +
                ", goodsName='" + goodsName + '\'' +
                ", goodsCode='" + goodsCode + '\'' +
                ", goodsUnit='" + goodsUnit + '\'' +
                ", price='" + price + '\'' +
                ", barCode='" + barCode + '\'' +
                ", source='" + source + '\'' +
                ", sourceUrl='" + sourceUrl + '\'' +
                ", spider='" + spider + '\'' +
                '}';
    }

    public String getUniscId() {
        return uniscId;
    }

    public void setUniscId(String uniscId) {
        this.uniscId = uniscId;
    }

    public String getEntName() {
        return entName;
    }

    public void setEntName(String entName) {
        this.entName = entName;
    }

    public String getGoodsNameImgUrl() {
        return goodsNameImgUrl;
    }

    public void setGoodsNameImgUrl(String goodsNameImgUrl) {
        this.goodsNameImgUrl = goodsNameImgUrl;
    }

    public String getGoodsName() {
        return goodsName;
    }

    public void setGoodsName(String goodsName) {
        this.goodsName = goodsName;
    }

    public String getGoodsCode() {
        return goodsCode;
    }

    public void setGoodsCode(String goodsCode) {
        this.goodsCode = goodsCode;
    }

    public String getGoodsUnit() {
        return goodsUnit;
    }

    public void setGoodsUnit(String goodsUnit) {
        this.goodsUnit = goodsUnit;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public String getBarCode() {
        return barCode;
    }

    public void setBarCode(String barCode) {
        this.barCode = barCode;
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
