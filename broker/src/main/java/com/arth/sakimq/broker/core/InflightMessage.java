package com.arth.sakimq.broker.core;

public record InflightMessage(String messageId, String receiptHandle, long deadline) {
}
