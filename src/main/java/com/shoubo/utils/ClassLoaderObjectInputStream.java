package com.shoubo.utils;

import java.io.*;
import java.lang.reflect.Proxy;

/**
 * @author shoubo
 * @date 008 23/6/8
 * @desc 一个 ObjectInputStream，它使用指定的类加载器而不是默认的类加载器加载类。
 */
public class ClassLoaderObjectInputStream extends ObjectInputStream {

    /** 要使用的类加载器 **/
    private final ClassLoader classLoader;


    /**
     * 构造新的 ClassLoaderObjectInputStream。
     * @param classLoader 从中加载类的类加载器
     * @param in 要使用的 InputStream
     * @throws IOException in case of an I/O error
     * @throws StreamCorruptedException if the stream is corrupted
     */
    public ClassLoaderObjectInputStream(ClassLoader classLoader, InputStream in) throws IOException, StreamCorruptedException {
        super(in);
        this.classLoader = classLoader;
    }

    /**
     * 解析由描述符指定的类，使用指定的类加载器或超级类加载器。
     * @param objectStreamClass 类的描述符
     * @return 由 ObjectStreamClass 描述的 Class 对象
     * @throws IOException in case of an I/O error
     * @throws ClassNotFoundException if the Class cannot be found
     */
    @Override
    protected Class<?> resolveClass(ObjectStreamClass objectStreamClass) throws IOException, ClassNotFoundException {

        Class<?> clazz = Class.forName(objectStreamClass.getName(), false, classLoader);

        if (clazz != null) {
            return clazz;
        } else {
            return super.resolveClass(objectStreamClass);
        }
    }

    /**
     * 创建一个代理类，该代理类使用指定的类加载器或超级类加载器实现指定的接口。
     * @param interfaces 要实现的接口
     * @return 实现接口的代理类
     * @throws IOException in case of an I/O error
     * @throws ClassNotFoundException if the Class cannot be found
     */
    @Override
    protected Class<?> resolveProxyClass(String[] interfaces) throws IOException, ClassNotFoundException {
        Class<?>[] interfaceClasses = new Class<?>[interfaces.length];
        for (int i = 0; i < interfaces.length; i++) {
            interfaceClasses[i] = Class.forName(interfaces[i], false, classLoader);
        }
        try {
            return Proxy.getProxyClass(classLoader, interfaceClasses);
        } catch (IllegalArgumentException e) {
            return super.resolveProxyClass(interfaces);
        }
    }

}
