package com.natural.data.analyze.flink.portrait.entity;

public class EmailInfo {
    private String emailType;
    private Long count;
    private String groupField;

    public String getEmailType() {
        return emailType;
    }

    public void setEmailType(String emailType) {
        this.emailType = emailType;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getGroupField() {
        return groupField;
    }

    public void setGroupField(String groupField) {
        this.groupField = groupField;
    }

    @Override
    public String toString() {
        return "emailType=" + emailType + ",count=" + count + ",groupField=" + groupField;
    }
}
