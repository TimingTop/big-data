package com.natural.data.analyze.flink.portrait.map;

import com.natural.data.analyze.flink.portrait.entity.EmailInfo;
import com.natural.data.analyze.flink.portrait.util.EmailUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class EmailMap implements MapFunction<String, EmailInfo> {
    /**
     *
     * 使用用户表
     * @param s
     * @return
     * @throws Exception
     */
    @Override
    public EmailInfo map(String s) throws Exception {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        String[] userInfos = s.split(",");
        String userId = userInfos[0];
        String username = userInfos[1];
        String sex = userInfos[2];
        String telephone = userInfos[3];
        String email = userInfos[4];
        String age = userInfos[5];
        String registerTime = userInfos[6];
        String useType = userInfos[7];

        String emailType = EmailUtils.getEmailTypeBy(email);

        EmailInfo emailInfo = new EmailInfo();
        emailInfo.setEmailType(emailType);
        emailInfo.setCount(1L);
        emailInfo.setGroupField("==emailInfo==" + emailType);

        return emailInfo;
    }
}
