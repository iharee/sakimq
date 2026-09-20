package com.arth.sakimq.exception;

/**
 * WAL 恢复失败：记录损坏或读取错误，Broker 应拒绝启动（fail closed）而不是截断数据
 */
public class WalRecoveryException extends SakimqException {

    public WalRecoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
