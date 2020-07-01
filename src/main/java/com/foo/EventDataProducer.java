package com.foo;

import com.foo.util.MockEventDataProducer;

import java.util.concurrent.ExecutionException;

public class EventDataProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        MockEventDataProducer.produceEventData(1, null);
    }
}
