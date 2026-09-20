package com.arth.sakimq.broker.server;

import java.io.IOException;

/**
 * 将 Broker 核心暴露为 gRPC 服务
 */
public interface MQServer extends AutoCloseable {

    void start() throws IOException;

    /**
     * 阻塞当前线程，直到 server 终止，例如收到关闭信号
     *
     * @throws InterruptedException 线程中断异常
     */
    void awaitTermination() throws InterruptedException;

    void shutdown();

    int port();

    @Override
    void close();
}