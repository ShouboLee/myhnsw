package com.shoubo.exception;

/**
 * @author shoubo
 * @date 025 23/5/25
 * @desc 实现为Index类抛出的异常的基类 处理与索引操作相关的异常
 */
public abstract class IndexException extends RuntimeException {
    /**
     * 定义一个 serialVersionUID，用于序列化和反序列化对象时的版本控制。
     */
    private static final long serialVersionUID = 1L;

    /**
     * 构造一个IndexException
     */
    public IndexException() {}

    /**
     * 构造包含msg的IndexException
     *
     * @param msg 异常消息的细节
     */
    public IndexException(String msg) {
        super(msg);
    }

    /**
     * 构造携带msg和异常cause的IndexException
     * @param msg 异常消息的细节.
     * @param  cause the cause
     */
    public IndexException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
