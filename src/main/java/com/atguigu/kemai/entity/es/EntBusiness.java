package com.atguigu.kemai.entity.es;

public class EntBusiness {
    private String entId ;
    private String mainBusiness;
    private String businessKeyword;
    private String financingRound;
    private String certStaffMajor;
    private String taxAYear;
    private String biddingTitle;

    private String biddingRole;

    private String taxCertType;
    private String customsCredit;
    private String customsOpsType;
    private String customsIndustry;

    @Override
    public String toString() {
        return "EntBusiness{" +
                "entId='" + entId + '\'' +
                ", mainBusiness='" + mainBusiness + '\'' +
                ", businessKeyword='" + businessKeyword + '\'' +
                ", financingRound='" + financingRound + '\'' +
                ", certStaffMajor='" + certStaffMajor + '\'' +
                ", taxAYear='" + taxAYear + '\'' +
                ", biddingTitle='" + biddingTitle + '\'' +
                ", biddingRole='" + biddingRole + '\'' +
                ", taxCertType='" + taxCertType + '\'' +
                ", customsCredit='" + customsCredit + '\'' +
                ", customsOpsType='" + customsOpsType + '\'' +
                ", customsIndustry='" + customsIndustry + '\'' +
                '}';
    }

    public String getEntId() {
        return entId;
    }

    public void setEntId(String entId) {
        this.entId = entId;
    }

    public String getMainBusiness() {
        return mainBusiness;
    }

    public void setMainBusiness(String mainBusiness) {
        this.mainBusiness = mainBusiness;
    }

    public String getBusinessKeyword() {
        return businessKeyword;
    }

    public void setBusinessKeyword(String businessKeyword) {
        this.businessKeyword = businessKeyword;
    }

    public String getFinancingRound() {
        return financingRound;
    }

    public void setFinancingRound(String financingRound) {
        this.financingRound = financingRound;
    }

    public String getCertStaffMajor() {
        return certStaffMajor;
    }

    public void setCertStaffMajor(String certStaffMajor) {
        this.certStaffMajor = certStaffMajor;
    }

    public String getTaxAYear() {
        return taxAYear;
    }

    public void setTaxAYear(String taxAYear) {
        this.taxAYear = taxAYear;
    }

    public String getBiddingTitle() {
        return biddingTitle;
    }

    public void setBiddingTitle(String biddingTitle) {
        this.biddingTitle = biddingTitle;
    }

    public String getBiddingRole() {
        return biddingRole;
    }

    public void setBiddingRole(String biddingRole) {
        this.biddingRole = biddingRole;
    }

    public String getTaxCertType() {
        return taxCertType;
    }

    public void setTaxCertType(String taxCertType) {
        this.taxCertType = taxCertType;
    }

    public String getCustomsCredit() {
        return customsCredit;
    }

    public void setCustomsCredit(String customsCredit) {
        this.customsCredit = customsCredit;
    }

    public String getCustomsOpsType() {
        return customsOpsType;
    }

    public void setCustomsOpsType(String customsOpsType) {
        this.customsOpsType = customsOpsType;
    }

    public String getCustomsIndustry() {
        return customsIndustry;
    }

    public void setCustomsIndustry(String customsIndustry) {
        this.customsIndustry = customsIndustry;
    }
}
