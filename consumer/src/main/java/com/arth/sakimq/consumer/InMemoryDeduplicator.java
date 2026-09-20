package com.arth.sakimq.consumer;

import com.arth.sakimq.config.Config;
import com.arth.sakimq.exception.InvalidArgumentException;
import com.arth.sakimq.model.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存去重器，懒移除过期消息
 */
public final class InMemoryDeduplicator implements Deduplicator {

    private static final Logger log = LoggerFactory.getLogger(InMemoryDeduplicator.class);

    private static final Duration DEFAULT_RETENTION = Duration.ofHours(1);
    private static final int DEFAULT_PRUNE_THRESHOLD = 10_000;

    private record MessageKey(String queue, String messageId) {
    }

    private final ConcurrentHashMap<MessageKey, Long> processedAt = new ConcurrentHashMap<>();
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
            throw new InvalidArgumentException("invalid retention");
        }
        if (pruneThreshold <= 0) {
            throw new InvalidArgumentException("pruneThreshold must be positive");
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
        Long at = processedAt.get(new MessageKey(delivery.queue(), delivery.messageId()));
        boolean duplicate = at != null && System.currentTimeMillis() - at < retention.toMillis();
        if (duplicate) {
            log.debug("Dedup hit: queue={}, messageId={}", delivery.queue(), delivery.messageId());
        }
        return duplicate;
    }

    @Override
    public void markCommitted(Delivery delivery) {
        // 只在业务显式调用 ack 后记录（RPC 失败也记录，避免重投导致业务重复执行）
        // 业务失败抛异常且不调 ack 时不会记录，重投后仍正常消费
        processedAt.put(new MessageKey(delivery.queue(), delivery.messageId()), System.currentTimeMillis());
        log.debug("Dedup committed: queue={}, messageId={}", delivery.queue(), delivery.messageId());
        if (processedAt.size() > pruneThreshold) {
            prune();
        }
    }

    /**
     * 清理已超过 retention 的记录，防止内存无限增长
     */
    public void prune() {
        long cutoff = System.currentTimeMillis() - retention.toMillis();
        int before = processedAt.size();
        processedAt.entrySet().removeIf(entry -> entry.getValue() < cutoff);
        log.debug("Pruned {} stale dedup entries, remaining={}", before - processedAt.size(), processedAt.size());
    }
}
