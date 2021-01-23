package com.atguigu.kemai.entity.es;

public class EntContacts {


    private String entId;

    private String entName;

    //联系人渠道
    private String contactSource;

    @Override
    public String toString() {
        return "EntContacts{" +
                "entId='" + entId + '\'' +
                ", entName='" + entName + '\'' +
                ", contactSource='" + contactSource + '\'' +
                '}';
    }

    public String getEntId() {
        return entId;
    }

    public void setEntId(String entId) {
        this.entId = entId;
    }

    public String getEntName() {
        return entName;
    }

    public void setEntName(String entName) {
        this.entName = entName;
    }

    public String getContactSource() {
        return contactSource;
    }

    public void setContactSource(String contactSource) {
        this.contactSource = contactSource;
    }


}
