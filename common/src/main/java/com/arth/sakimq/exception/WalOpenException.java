package com.arth.sakimq.exception;

/**
 * WAL 打开失败
 */
public class WalOpenException extends SakimqException {

    public WalOpenException(String message, Throwable cause) {
        super(message, cause);
    }
}