package com.atguigu.kemai.entity.es;

public class EntNews {
//    """新闻

    private String entId;
    private String mediaTitle;
    private String newsTitles;

    @Override
    public String toString() {
        return "EntNews{" +
                "entId='" + entId + '\'' +
                ", mediaTitle='" + mediaTitle + '\'' +
                ", newsTitles='" + newsTitles + '\'' +
                '}';
    }

    public String getEntId() {
        return entId;
    }

    public void setEntId(String entId) {
        this.entId = entId;
    }

    public String getMediaTitle() {
        return mediaTitle;
    }

    public void setMediaTitle(String mediaTitle) {
        this.mediaTitle = mediaTitle;
    }

    public String getNewsTitles() {
        return newsTitles;
    }

    public void setNewsTitles(String newsTitles) {
        this.newsTitles = newsTitles;
    }
}
