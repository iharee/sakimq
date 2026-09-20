package com.arth.sakimq.exception;

/**
 * 目标队列不存在
 */
public class QueueNotFoundException extends SakimqException {

    public QueueNotFoundException(String queue) {
        super("Queue does not exist: " + queue);
    }
}