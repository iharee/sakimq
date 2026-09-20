package com.arth.sakimq.broker.config;

import com.arth.sakimq.config.Config;
import com.arth.sakimq.exception.InvalidArgumentException;

import java.nio.file.Path;

/**
 * @param port              gRPC 服务端口
 * @param dataDirectory     数据目录（WAL 等）
 * @param maxDeliveryCount  消息最多投递次数
 */
public record MQConfig(int port, Path dataDirectory, int maxDeliveryCount) {

    public MQConfig {
        if (maxDeliveryCount <= 0) {
            throw new InvalidArgumentException("maxDeliveryCount must be positive");
        }
        // port 允许 0：gRPC 会分配临时端口
        if (port < 0 || port > 65535) {
            throw new InvalidArgumentException("port must be between 0 and 65535");
        }
        if (dataDirectory == null) {
            throw new InvalidArgumentException("dataDirectory must not be null");
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
