package com.shoubo.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author shoubo
 * @date 23/5/25
 * @desc 提供一个具有命名模式的线程工厂，用于创建线程对象。
 */
public class NamedThreadFactory implements ThreadFactory {
    private final String namingPattern;

    private final AtomicInteger counter;

    public NamedThreadFactory(String namingPattern) {
        this.namingPattern = namingPattern;
        this.counter = new AtomicInteger(0);
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, String.format(namingPattern, counter.incrementAndGet()));
    }

}
