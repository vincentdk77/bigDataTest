package com.atguigu.kemai.entity.es;

public class EntEcommerce {

    private String entId;

    private String ecPlatform;//电商上架平台

    private String ecMainProduct;//电商主营产品

    private String itemCategories;//电商商品分类

    private String shopName;

    private String ecBrand;//电商主营品牌

    private String ecShopFoundTime;//电商店铺创建时间

    private String shopRate;//电商店铺平均分

    @Override
    public String toString() {
        return "EntEcommerce{" +
                "entId='" + entId + '\'' +
                ", ecPlatform='" + ecPlatform + '\'' +
                ", ecMainProduct='" + ecMainProduct + '\'' +
                ", itemCategories='" + itemCategories + '\'' +
                ", shopName='" + shopName + '\'' +
                ", ecBrand='" + ecBrand + '\'' +
                ", ecShopFoundTime='" + ecShopFoundTime + '\'' +
                ", shopRate='" + shopRate + '\'' +
                '}';
    }

    public String getEntId() {
        return entId;
    }

    public void setEntId(String entId) {
        this.entId = entId;
    }

    public String getEcPlatform() {
        return ecPlatform;
    }

    public void setEcPlatform(String ecPlatform) {
        this.ecPlatform = ecPlatform;
    }

    public String getEcMainProduct() {
        return ecMainProduct;
    }

    public void setEcMainProduct(String ecMainProduct) {
        this.ecMainProduct = ecMainProduct;
    }

    public String getItemCategories() {
        return itemCategories;
    }

    public void setItemCategories(String itemCategories) {
        this.itemCategories = itemCategories;
    }

    public String getShopName() {
        return shopName;
    }

    public void setShopName(String shopName) {
        this.shopName = shopName;
    }

    public String getEcBrand() {
        return ecBrand;
    }

    public void setEcBrand(String ecBrand) {
        this.ecBrand = ecBrand;
    }

    public String getEcShopFoundTime() {
        return ecShopFoundTime;
    }

    public void setEcShopFoundTime(String ecShopFoundTime) {
        this.ecShopFoundTime = ecShopFoundTime;
    }

    public String getShopRate() {
        return shopRate;
    }

    public void setShopRate(String shopRate) {
        this.shopRate = shopRate;
    }
}
