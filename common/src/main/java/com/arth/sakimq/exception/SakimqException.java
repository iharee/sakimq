package com.arth.sakimq.exception;

public abstract class SakimqException extends RuntimeException {

    public SakimqException(String message) {
        super(message);
    }

    public SakimqException(String message, Throwable cause) {
        super(message, cause);
    }
}