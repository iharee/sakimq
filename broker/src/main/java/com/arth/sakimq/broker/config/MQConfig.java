package com.arth.sakimq.broker.config;

import java.nio.file.Path;

public record MQConfig(int port, Path dataDirectory) {

    public static MQConfig defaults() {
        return new MQConfig(50051, Path.of("data"));
    }
}
