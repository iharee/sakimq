package com.arth.sakimq.exception;

/**
 * 发布重复的 messageId
 */
public class DuplicateMessageException extends SakimqException {

    public DuplicateMessageException(String messageId) {
        super("duplicate message id: " + messageId);
    }
}