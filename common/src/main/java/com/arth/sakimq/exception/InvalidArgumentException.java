package com.arth.sakimq.exception;

/**
 * 参数非法：调用方传入的参数不符合预期
 */
public class InvalidArgumentException extends SakimqException {

    public InvalidArgumentException(String message) {
        super(message);
    }

    public InvalidArgumentException(String message, Throwable cause) {
        super(message, cause);
    }
}