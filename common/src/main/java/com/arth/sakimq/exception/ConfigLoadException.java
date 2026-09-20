package com.arth.sakimq.exception;

/**
 * 配置文件读取失败
 */
public class ConfigLoadException extends SakimqException {

    public ConfigLoadException(String message, Throwable cause) {
        super(message, cause);
    }
}