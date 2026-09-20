package com.arth.sakimq.exception;

/**
 * WAL 恢复失败：记录损坏或读取错误，Broker 拒绝启动
 */
public class WalRecoveryException extends SakimqException {

    public WalRecoveryException(String message) {
        super(message);
    }

    public WalRecoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
