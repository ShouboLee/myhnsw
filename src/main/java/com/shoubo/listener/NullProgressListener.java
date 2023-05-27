package com.shoubo.listener;

/**
 * Author: shoubo
 * Date: 2023/5/24
 * Desc: 定义了一个名为 NullProgressListener 的类，它实现了 ProgressListener 接口，并提供了一个什么都不做的实现。
 */
public class NullProgressListener implements ProgressListener {

    /*
    * NullProgressListener 的一个单例实例，用于方便使用。由于进度监听器本身没有状态，因此可以共享一个实例。
    * */
    public static final NullProgressListener INSTANCE = new NullProgressListener();

    /*
    * NullProgressListener 的构造函数是私有的，这样就不允许直接实例化该类的对象，只能通过使用预定义的单例实例 INSTANCE。
    * */
    private NullProgressListener() {
    }

    @Override
    public void updateProgress(int workDone, int max) {
        // do nothing
    }
}
