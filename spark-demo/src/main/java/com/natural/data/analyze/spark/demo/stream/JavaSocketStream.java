package com.natural.data.analyze.spark.demo.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Pattern;


/**
 *   先启动  nc -lk 8899
 *
 *
 *
 *
 */

public class JavaSocketStream {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("javasocket")
                .setMaster("local");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));
        // 自定义一个创建 streaming 的 receiver
        JavaReceiverInputDStream<String> lines = ssc.receiverStream(new JavaCustomReceiver("localhost", 8899));
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((x1, x2) -> x1 + x2);

        System.out.println("aaa");
        wordCounts.print();

        ssc.start();
        ssc.awaitTermination();
    }

    static class JavaCustomReceiver extends Receiver<String> {

        String host = null;
        int port = -1;

        public JavaCustomReceiver(String host_, int port_) {
            super(StorageLevel.MEMORY_AND_DISK_2());
            host = host_;
            port = port_;
        }

        @Override
        public void onStart() {
            new Thread(this::receive).start();
        }

        @Override
        public void onStop() {

        }

        private void receive() {

            try {
                Socket socket = null;
                BufferedReader reader = null;
                try {


                    socket = new Socket(host, port);
                    reader = new BufferedReader(
                            new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8)
                    );

                    String userInput;

                    while(!isStopped() && (userInput = reader.readLine()) != null) {
                        System.out.println("Received data : " + userInput);
                        store(userInput);
                    }


                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    reader.close();
                    socket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }


        }
    }
}
