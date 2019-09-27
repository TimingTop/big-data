package com.natural.data.analyze.spark;

import com.google.common.util.concurrent.RateLimiter;

public class Application {

    public static void main(String[] args) {

        RateLimiter limit = RateLimiter.create(100);

    }
}
