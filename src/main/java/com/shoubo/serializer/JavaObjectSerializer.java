package com.shoubo.serializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author shoubo
 * @date 028 23/5/28
 * @desc 实现序列化接口
 *
 * @param <T> 序列化对象的类型
 */
public class JavaObjectSerializer<T> implements ObjectSerializer<T>{

    /**
     * 定义一个 serialVersionUID，用于序列化和反序列化对象时的版本控制。
     */
    private static final long serialVersionUID = 1L;

    /**
     * 将item写入ObjectOutput实现
     *
     * @param item 序列化对象
     * @param out  ObjectOutput实现
     * @throws IOException I/O exception
     */
    @Override
    public void write(T item, ObjectOutput out) throws IOException {
        out.writeObject(item);
    }

    /**
     * 从ObjectInput实现中读取item
     * @param in ObjectInput实现
     * @return 读取的item
     * @throws IOException I/O exception
     * @throws ClassNotFoundException 读取的item与item的类型不匹配
     */
    @Override
    public T read(ObjectInput in) throws IOException, ClassNotFoundException {
        return (T) in.readObject();
    }
}
