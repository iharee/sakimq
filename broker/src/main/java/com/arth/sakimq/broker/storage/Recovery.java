package com.arth.sakimq.broker.storage;

import com.arth.sakimq.broker.core.Broker;
import com.arth.sakimq.broker.core.Message;
import com.arth.sakimq.protocol.WalRecord;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public final class Recovery {

    private Recovery() {
    }

    /** messageId 的唯一性域是 queue 内（{@code MessageQueue.messageIds} 按队列隔离），故用复合键 */
    private record MessageKey(String queue, String messageId) {
    }

    public static void recover(MessageLog wal, Broker broker) {
        Map<MessageKey, Message> pending = new LinkedHashMap<>();
        Map<MessageKey, Integer> deliveryCounts = new HashMap<>();
        for (WalRecord record : wal.recover()) {
            String queue = record.getQueue();
            // 兼容旧版 WAL：早期版本没有 CREATE_QUEUE 记录，任何出现过活动的队列都应在重启后恢复。
            // 完全没有任何记录的"真空队列"信息从未落盘，无法恢复，属旧格式固有限制。
            if (!queue.isBlank()) {
                broker.restoreQueue(queue);
            }
            MessageKey key = new MessageKey(queue, record.getMessageId());
            switch (record.getType()) {
                case PUBLISH -> {
                    pending.put(key, new Message(
                            record.getMessageId(),
                            queue,
                            record.getBody().toByteArray(),
                            record.getCreatedAt()));
                    deliveryCounts.put(key, record.getDeliveryCount());
                }
                case DELIVERY -> deliveryCounts.put(key, record.getDeliveryCount());
                case ACK -> {
                    pending.remove(key);
                    deliveryCounts.remove(key);
                }
                default -> {
                    // CREATE_QUEUE 已在上方统一恢复；未知类型忽略
                }
            }
        }
        for (Map.Entry<MessageKey, Message> entry : pending.entrySet()) {
            broker.restore(entry.getValue(), deliveryCounts.getOrDefault(entry.getKey(), 0));
        }
    }
}
