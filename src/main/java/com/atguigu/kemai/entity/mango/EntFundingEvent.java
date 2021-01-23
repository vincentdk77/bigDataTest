package com.atguigu.kemai.entity.mango;

public class EntFundingEvent {

    //融资历史

    private String vcName;//  # 投资方 多个逗号分隔
    private String pubDate ;// # 融资日期 2020-06-06
    private String input_processor;

    private String amtDisplayed ;// # 融资金额 显示字段
    private String amtCurCN ;// # 货币单位： 人民币/美元
    private String amt ;// # 金额数值
    private String stag ;//  # E轮
    private String valueDisplay ;//  # 估值
    private String source ;
    private String sourceUrl;
    private String spider ;
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

    public String getVcName() {
        return vcName;
    }

    public void setVcName(String vcName) {
        this.vcName = vcName;
    }

    public String getPubDate() {
        return pubDate;
    }

    public void setPubDate(String pubDate) {
        this.pubDate = pubDate;
    }

    public String getInput_processor() {
        return input_processor;
    }

    public void setInput_processor(String input_processor) {
        this.input_processor = input_processor;
    }

    public String getAmtDisplayed() {
        return amtDisplayed;
    }

    public void setAmtDisplayed(String amtDisplayed) {
        this.amtDisplayed = amtDisplayed;
    }

    public String getAmtCurCN() {
        return amtCurCN;
    }

    public void setAmtCurCN(String amtCurCN) {
        this.amtCurCN = amtCurCN;
    }

    public String getAmt() {
        return amt;
    }

    public void setAmt(String amt) {
        this.amt = amt;
    }

    public String getStag() {
        return stag;
    }

    public void setStag(String stag) {
        this.stag = stag;
    }

    public String getValueDisplay() {
        return valueDisplay;
    }

    public void setValueDisplay(String valueDisplay) {
        this.valueDisplay = valueDisplay;
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
