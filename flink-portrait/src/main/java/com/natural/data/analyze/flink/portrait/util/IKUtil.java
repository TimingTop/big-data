package com.natural.data.analyze.flink.portrait.util;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class IKUtil {

    private static Analyzer analyzer = new IKAnalyzer(true);

    public static List<String> getIKWord(String words) {
        List<String> resultList = new ArrayList<>();
        StringReader reader = new StringReader(words);

        TokenStream ts = null;

        ts = analyzer.tokenStream("", reader);
        CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);

        try {
            ts.reset();
            while (ts.incrementToken()) {
                String result = term.toString();
                resultList.add(result);
            }
            ts.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        reader.close();
        return resultList;
    }
}
