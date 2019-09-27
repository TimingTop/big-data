package com.natural.data.analyze.spark.height;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;

public class AvageHeight {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("caculator").setMaster("local[2]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> dataFile = context.textFile("C:\\personal\\big-data-root\\big-data-spark\\src\\main\\resources\\com\\natural\\data\\analyze\\spark\\height\\PeopleInfo.txt");

        JavaRDD<String> maleFilterData = dataFile.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("M");
            }
        });

        JavaRDD<String> femaleFilterData = dataFile.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("F");
            }
        });

        // 得到男 的身高数据
        JavaRDD<String> maleHeightData = maleFilterData.flatMap(new FlatMapFunction<String, String>() {


            public Iterator<String> call(String s) throws Exception {

                return Arrays.asList(s.split(" ")[2]).iterator();
            }
        });

        JavaRDD<String> femaleHeightData = femaleFilterData.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")[2]).iterator();
            }
        });


        JavaRDD<Integer> maleHeightDataInt = maleHeightData.map(new Function<String, Integer>() {
            public Integer call(String s) throws Exception {
                return Integer.parseInt(s);
            }
        });

        JavaRDD<Integer> femaleHeightDataInt = femaleHeightData.map(new Function<String, Integer>() {
            public Integer call(String s) throws Exception {
                return Integer.parseInt(s);
            }
        });

        JavaRDD<Integer> maleHeightLowSort = maleHeightDataInt.sortBy(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                return integer;
            }
        }, true, 3);

        JavaRDD<Integer> femaleHeightLowSort = femaleHeightDataInt.sortBy(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                return integer;
            }
        }, true, 3);

        JavaRDD<Integer> maleHeightHighSort = maleHeightDataInt.sortBy(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                return integer;
            }
        }, false, 3);

        JavaRDD<Integer> femaleHeightHighSort = femaleHeightDataInt.sortBy(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                return integer;
            }
        }, false, 3);

        Integer lowestMale = maleHeightLowSort.first();
        Integer highestMale = maleHeightHighSort.first();

        Integer lowestFemale = femaleHeightLowSort.first();
        Integer highestFemale = femaleHeightHighSort.first();



        System.out.println(("Female number: " + femaleHeightData.count()));
        System.out.println("Male number: " + maleHeightData.count());
        System.out.println("Lowest Male: " + lowestMale);
        System.out.println("Lowest Female: " + lowestFemale);
        System.out.println("Highest Male: " + highestMale);
        System.out.println("Highest Female: " + highestFemale);



    }
}
