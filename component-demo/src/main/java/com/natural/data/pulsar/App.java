package com.natural.data.pulsar;

import java.util.Arrays;
import java.util.List;

public class App {

    public static void main(String[] args) {

        List.of("aaa", "bbb")
                .stream()
                .filter(s -> s.startsWith("a"))
                .mapToInt(String::length)
                .sorted()
                .max();
    }
}
