package com.natural.data.analyze.flink.portrait.util;

public class EmailUtils {
    public static String getEmailTypeBy(String email) {
        String emailType = "其他邮箱用户";
        if(email.contains("@163.com")||email.contains("@126.com")){
            emailType = "网易邮箱用户";
        }else if (email.contains("@139.com")){
            emailType = "移动邮箱用户";
        }else if (email.contains("@sohu.com")){
            emailType = "搜狐邮箱用户";
        }else if (email.contains("@qq.com")){
            emailType = "qq邮箱用户";
        }else if (email.contains("@189.cn")){
            emailType = "189邮箱用户";
        }else if (email.contains("@tom.com")){
            emailType = "tom邮箱用户";
        }else if (email.contains("@aliyun.com")){
            emailType = "阿里邮箱用户";
        }else if (email.contains("@sina.com")){
            emailType = "新浪邮箱用户";
        }
        return emailType;
    } 
}
