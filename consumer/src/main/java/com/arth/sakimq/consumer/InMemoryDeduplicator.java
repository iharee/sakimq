package com.arth.sakimq.consumer;

import com.arth.sakimq.config.Config;
import com.arth.sakimq.model.Delivery;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存去重器，懒移除过期消息
 */
public final class InMemoryDeduplicator implements Deduplicator {

    private static final Duration DEFAULT_RETENTION = Duration.ofHours(1);
    private static final int DEFAULT_PRUNE_THRESHOLD = 10_000;

    private final ConcurrentHashMap<String, Long> processedAt = new ConcurrentHashMap<>();
    private final Duration retention;
    private final int pruneThreshold;

    public InMemoryDeduplicator() {
        this(DEFAULT_RETENTION, DEFAULT_PRUNE_THRESHOLD);
    }

    public InMemoryDeduplicator(Duration retention) {
        this(retention, DEFAULT_PRUNE_THRESHOLD);
    }

    public InMemoryDeduplicator(Duration retention, int pruneThreshold) {
        if (retention == null || retention.isNegative() || retention.isZero()) {
            throw new IllegalArgumentException("invalid retention");
        }
        if (pruneThreshold <= 0) {
            throw new IllegalArgumentException("pruneThreshold must be positive");
        }
        this.retention = retention;
        this.pruneThreshold = pruneThreshold;
    }

    public static InMemoryDeduplicator from(Config config) {
        return new InMemoryDeduplicator(
                config.getDuration("sakimq.consumer.deduplicator.retention", DEFAULT_RETENTION),
                config.getInt("sakimq.consumer.deduplicator.prune-threshold", DEFAULT_PRUNE_THRESHOLD));
    }

    @Override
    public boolean isDuplicate(Delivery delivery) {
        Long at = processedAt.get(delivery.messageId());
        return at != null && System.currentTimeMillis() - at < retention.toMillis();
    }

    @Override
    public void markCommitted(Delivery delivery) {
        // 只在业务显式 ack 成功后才应记录 messageId，否则业务失败后重投会被自动 ACK 并永久删除
        processedAt.put(delivery.messageId(), System.currentTimeMillis());
        if (processedAt.size() > pruneThreshold) {
            prune();
        }
    }

    /**
     * 清理已超过 retention 的记录，防止内存无限增长
     */
    public void prune() {
        long cutoff = System.currentTimeMillis() - retention.toMillis();
        processedAt.entrySet().removeIf(entry -> entry.getValue() < cutoff);
    }
}
