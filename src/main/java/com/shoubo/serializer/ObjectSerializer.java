package com.shoubo.serializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * @author shoubo
 * @date 23/5/28
 * @desc 定义序列化接口 被持久化的Index对象序列化方式
 * @param <T> 序列化对象的类型
 */
public interface ObjectSerializer<T> extends Serializable {
    /**
     * 将item写入ObjectOutput实现
     * @param item 序列化对象
     * @param out ObjectOutput实现
     * @throws IOException I/O exception
     */
    void write(T item, ObjectOutput out) throws IOException;

    /**
     * 从ObjectInput实现中读取item
     * @param in ObjectInput实现
     * @return 读取的item
     * @throws IOException I/O exception
     * @throws ClassNotFoundException 读取的item与item的类型不匹配
     */
    T read(ObjectInput in) throws IOException, ClassNotFoundException;
}
