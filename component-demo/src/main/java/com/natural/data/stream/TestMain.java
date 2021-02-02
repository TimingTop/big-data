package com.natural.data.stream;

public class TestMain {

    public static void main(String[] args) {

        new MyStream(true, false)
                .lazy(() -> {System.out.println("A");})
                .lazy(() -> {System.out.println("b");})
                .lazy(() -> {System.out.println("c");})
                .lazy(() -> {System.out.println("d");})
                .lazy(() -> {System.out.println("e");})
                .lazy(() -> {System.out.println("f");})
                .lazy(() -> {System.out.println("g");})
                .now(() -> System.out.println("end"));

    }
}
