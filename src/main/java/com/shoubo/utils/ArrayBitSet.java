package com.shoubo.utils;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author shoubo
 * @date 23/5/28
 * @desc 实现一个位图用于存储Index 从而减少索引的创建和销毁提高性能 但是会增加内存的消耗 适用于索引创建和销毁代价较大的情况
 * 位图的实现参考了Lucene的OpenBitSet 但是没有使用long数组而是使用int数组 从而减少内存的消耗 但是会增加索引的数量限制 适用于索引数量较少的情况
 */
public class ArrayBitSet implements Serializable {

    /**
     * 定义一个 serialVersionUID，用于序列化和反序列化对象时的版本控制。
     */
    private static final long serialVersionUID = 1L;

    /**
     * 位图的缓冲区
     */
    private final int[] buffer;

    /**
     * 构造一个位图
     * @param size 位图的大小
     */
    public ArrayBitSet(int size) {
        this.buffer = new int[(size >> 5) + 1];
    }

    /**
     * 构造一个位图
     * @param other 用于复制的位图
     * @param count 位图的大小
     */
    public ArrayBitSet(ArrayBitSet other, int count) {
        this.buffer = Arrays.copyOf(other.buffer, (count >> 5) + 1);
    }

    /**
     * 判断位图中是否包含某个索引
     * @param id 索引
     * @return 是否包含
     */
    public boolean contains(int id) {
        int carrier = this.buffer[id >> 5];
        return ((1 << (id & 31)) & carrier) != 0;
    }

    /**
     * 添加一个索引到位图中
     * @param id 索引
     */
    public void add(int id) {
        int mask = 1 << (id & 31);
        this.buffer[id >> 5] |= mask;
    }

    /**
     * 从位图中移除一个索引
     * @param id 索引
     */
    public void remove(int id) {
        int mask = 1 << (id & 31);
        this.buffer[id >> 5] &= ~mask;
    }

    /**
     * 清空位图
     */
    public void clear() {
        Arrays.fill(this.buffer, 0);
    }
}
