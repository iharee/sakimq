package com.arth.sakimq.broker.config;

import com.arth.sakimq.config.Config;

import java.nio.file.Path;

/**
 * @param port              gRPC 服务端口
 * @param dataDirectory     数据目录（WAL 等）
 * @param maxDeliveryCount  消息最多投递次数；超过后默认丢弃（死信队列 TODO）
 */
public record MQConfig(int port, Path dataDirectory, int maxDeliveryCount) {

    public MQConfig {
        if (maxDeliveryCount <= 0) {
            throw new IllegalArgumentException("maxDeliveryCount must be positive");
        }
    }

    public static MQConfig defaults() {
        return new MQConfig(50051, Path.of("data"), 3);
    }

    public static MQConfig from(Config config) {
        return new MQConfig(
                config.getInt("sakimq.broker.port", 50051),
                config.getPath("sakimq.broker.data-directory", Path.of("data")),
                config.getInt("sakimq.broker.max-delivery-count", 3));
    }
}
