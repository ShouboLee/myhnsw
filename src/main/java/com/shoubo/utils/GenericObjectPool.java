package com.shoubo.utils;

import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Supplier;

/**
 * @author shoubo
 * @date 23/5/28
 * @desc 实现一个对象池用于存储对象 从而减少对象的创建和销毁提高性能
 * 但是会增加内存的消耗 适用于对象创建和销毁代价较大的情况
 * @param <T> 对象池中对象的类型
 */
public class GenericObjectPool<T> implements Serializable {

    /**
     * 定义一个 serialVersionUID，用于序列化和反序列化对象时的版本控制。
     */
    private static final long serialVersionUID = 1L;

    /**
     * 对象池中的对象
     */
    private final ArrayBlockingQueue<T> items;

    /**
     * 构造一个对象池
     * @param supplier 用于创建对象池中对象的工厂
     * @param capacity 对象池的容量
     */
    public GenericObjectPool(Supplier<T> supplier, int capacity) {
        this.items = new ArrayBlockingQueue<>(capacity);

        for (int i = 0; i < capacity; i++) {
            items.add(supplier.get());
        }
    }

    /**
     * 从对象池中借出一个对象
     * @return 借出的对象
     */
    public T borrowObject() {
        try {
            return items.take();
        } catch (InterruptedException e) {
            throw new RuntimeException("从对象池中取出对象异常", e);  // TODO any more elegant way to do this ?
        }
    }

    /**
     * 将对象归还给对象池
     * @param item 归还的对象
     */
    public void returnObject(T item) {
        items.add(item);
    }
}
