package com.atguigu.kemai.entity.es;

public class EntRecruit {

    private String entId;

    private String recruitTitle;

    private String jobDesc;

    private String salary;

    private String recruitAddr;

    private String recruitDetailAddr;

    private String postType;//岗位类型

    private String recruitTitleLM;//最近1个月招聘岗位名称

    private String city;

    private String province;

    private String district;



    @Override
    public String toString() {
        return "EntRecruit{" +
                "entId='" + entId + '\'' +
                ", recruitTitle='" + recruitTitle + '\'' +
                ", jobDesc='" + jobDesc + '\'' +
                ", salary='" + salary + '\'' +
                ", recruitAddr='" + recruitAddr + '\'' +
                ", recruitDetailAddr='" + recruitDetailAddr + '\'' +
                ", postType='" + postType + '\'' +
                ", recruitTitleLM='" + recruitTitleLM + '\'' +
                '}';
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getDistrict() {
        return district;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    public String getEntId() {
        return entId;
    }

    public void setEntId(String entId) {
        this.entId = entId;
    }

    public String getRecruitTitle() {
        return recruitTitle;
    }

    public void setRecruitTitle(String recruitTitle) {
        this.recruitTitle = recruitTitle;
    }

    public String getJobDesc() {
        return jobDesc;
    }

    public void setJobDesc(String jobDesc) {
        this.jobDesc = jobDesc;
    }

    public String getSalary() {
        return salary;
    }

    public void setSalary(String salary) {
        this.salary = salary;
    }

    public String getRecruitAddr() {
        return recruitAddr;
    }

    public void setRecruitAddr(String recruitAddr) {
        this.recruitAddr = recruitAddr;
    }

    public String getRecruitDetailAddr() {
        return recruitDetailAddr;
    }

    public void setRecruitDetailAddr(String recruitDetailAddr) {
        this.recruitDetailAddr = recruitDetailAddr;
    }

    public String getPostType() {
        return postType;
    }

    public void setPostType(String postType) {
        this.postType = postType;
    }

    public String getRecruitTitleLM() {
        return recruitTitleLM;
    }

    public void setRecruitTitleLM(String recruitTitleLM) {
        this.recruitTitleLM = recruitTitleLM;
    }
}
