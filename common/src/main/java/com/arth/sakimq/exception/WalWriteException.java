package com.arth.sakimq.exception;

/**
 * WAL 写入失败
 */
public class WalWriteException extends SakimqException {

    public WalWriteException(String message) {
        super(message);
    }

    public WalWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}