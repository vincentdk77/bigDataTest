package com.atguigu.kemai.entity.es;

public class EntBrand {
    private String entId;
    private String brandName;
    private String brandIndustry;
    private String brandProduct;

    @Override
    public String toString() {
        return "EntBrand{" +
                "entId='" + entId + '\'' +
                ", brandName='" + brandName + '\'' +
                ", brandIndustry='" + brandIndustry + '\'' +
                ", brandProduct='" + brandProduct + '\'' +
                '}';
    }

    public String getEntId() {
        return entId;
    }

    public void setEntId(String entId) {
        this.entId = entId;
    }

    public String getBrandName() {
        return brandName;
    }

    public void setBrandName(String brandName) {
        this.brandName = brandName;
    }

    public String getBrandIndustry() {
        return brandIndustry;
    }

    public void setBrandIndustry(String brandIndustry) {
        this.brandIndustry = brandIndustry;
    }

    public String getBrandProduct() {
        return brandProduct;
    }

    public void setBrandProduct(String brandProduct) {
        this.brandProduct = brandProduct;
    }
}
