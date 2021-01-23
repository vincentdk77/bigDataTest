package com.atguigu.kemai.entity.mango;

public class EntRecruit {

    // 招聘信息

    //   # 岗位名称
    private String title;
    // # 市/县
    private String city;
    // # 区
    private String district;
    // # 薪资范围
    private String salary;

    private String salaryFrom ;

    private String salaryTo ;
    //# 发布日期
    private String rdate;
    // # 来源名称
    private String source;
    //# 来源地址
    private String sourceUrl;
    // # 爬虫name
    private String spider;

    private String degree;

    private String jobDesc;

    //       # 全站抓取时关联用
    private String entName;

    private String createTime;
    private String updateTime;

    private String postType;

    public String getPostType() {
        return postType;
    }

    public void setPostType(String postType) {
        this.postType = postType;
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
        return "EntRecruit{" +
                "title='" + title + '\'' +
                ", city='" + city + '\'' +
                ", district='" + district + '\'' +
                ", salary='" + salary + '\'' +
                ", salaryFrom='" + salaryFrom + '\'' +
                ", salaryTo='" + salaryTo + '\'' +
                ", rdate='" + rdate + '\'' +
                ", source='" + source + '\'' +
                ", sourceUrl='" + sourceUrl + '\'' +
                ", spider='" + spider + '\'' +
                ", degree='" + degree + '\'' +
                ", jobDesc='" + jobDesc + '\'' +
                ", entName='" + entName + '\'' +
                '}';
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getDistrict() {
        return district;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    public String getSalary() {
        return salary;
    }

    public void setSalary(String salary) {
        this.salary = salary;
    }

    public String getSalaryFrom() {
        return salaryFrom;
    }

    public void setSalaryFrom(String salaryFrom) {
        this.salaryFrom = salaryFrom;
    }

    public String getSalaryTo() {
        return salaryTo;
    }

    public void setSalaryTo(String salaryTo) {
        this.salaryTo = salaryTo;
    }

    public String getRdate() {
        return rdate;
    }

    public void setRdate(String rdate) {
        this.rdate = rdate;
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

    public String getDegree() {
        return degree;
    }

    public void setDegree(String degree) {
        this.degree = degree;
    }

    public String getJobDesc() {
        return jobDesc;
    }

    public void setJobDesc(String jobDesc) {
        this.jobDesc = jobDesc;
    }

    public String getEntName() {
        return entName;
    }

    public void setEntName(String entName) {
        this.entName = entName;
    }
}
