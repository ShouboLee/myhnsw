package com.shoubo.exception;

/**
 * @author shoubo
 * @date 006 23/6/6
 * @desc 抛出 SizeLimitExceededException 异常 表示索引的大小已超出限制。
 */
public class SizeLimitExceededException extends IndexException{
    private static final long serialVersionUID = 1L;

    /**
     * 构造一个 SizeLimitExceededException 异常 带有详细的message信息。
     *
     * @param message the detail message.
     */
    public SizeLimitExceededException(String message) {
        super(message);
    }
}
