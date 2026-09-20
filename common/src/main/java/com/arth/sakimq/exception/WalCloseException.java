package com.arth.sakimq.exception;

/**
 * WAL 关闭失败
 */
public class WalCloseException extends SakimqException {

    public WalCloseException(String message, Throwable cause) {
        super(message, cause);
    }
}