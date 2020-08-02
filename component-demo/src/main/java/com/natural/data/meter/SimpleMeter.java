package com.natural.data.meter;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.cumulative.CumulativeCounter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.List;

public class SimpleMeter {

    public static void main(String[] args) {


        MeterRegistry registry = new SimpleMeterRegistry();

        // 计算请求数      6 times
        registry.counter("http.requests", "uri", "api").increment();
        registry.counter("http.requests", "uri", "api").increment();
        registry.counter("http.requests", "uri", "api").increment();
        registry.counter("http.requests", "uri", "api").increment();
        registry.counter("http.requests", "uri", "api").increment();
        registry.counter("http.requests", "uri", "api").increment();

        Counter.builder("http.requests2")
                .description("This is a new Counter.")
                .tags("uri", "api")
                .register(registry);


        List<Meter> meters = registry.getMeters();
        for (Meter meter : meters) {
            System.out.println(((CumulativeCounter) meter).count());
        }
    }
}
