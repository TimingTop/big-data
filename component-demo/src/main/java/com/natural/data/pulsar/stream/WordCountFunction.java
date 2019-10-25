package com.natural.data.pulsar.stream;


import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.util.Arrays;
import java.util.Collection;

public class WordCountFunction implements Function<String, Void> {

    @Override
    public Void process(String s, Context context) throws Exception {


        Collection<String> inputTopics = context.getInputTopics();


        Arrays.asList(s.split(" "))
                .forEach(x -> {
                    String counterKey = x.toLowerCase();
                    context.incrCounter(counterKey, 1);
                });
        return null;
    }
}
