package com.natural.data.analyze.flink.portrait.reduce;

import com.natural.data.analyze.flink.portrait.entity.EmailInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

public class EmailReduce implements ReduceFunction<EmailInfo> {
    @Override
    public EmailInfo reduce(EmailInfo emailInfo, EmailInfo t1) throws Exception {
        String emailType = emailInfo.getEmailType();
        Long count1 = emailInfo.getCount();
        Long count2 = t1.getCount();

        EmailInfo result = new EmailInfo();
        result.setEmailType(emailType);
        result.setCount(count1 +count2);

        return result;
    }
}
