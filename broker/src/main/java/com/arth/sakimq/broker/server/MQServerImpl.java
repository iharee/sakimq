package com.arth.sakimq.broker.server;

import com.arth.sakimq.broker.config.MQConfig;
import com.arth.sakimq.broker.core.Broker;
import com.arth.sakimq.broker.core.MonoBroker;
import com.arth.sakimq.broker.storage.FileWal;
import com.arth.sakimq.broker.storage.MessageLog;
import com.arth.sakimq.broker.storage.Recovery;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.concurrent.Executors;


public final class MQServerImpl implements MQServer {

    private final MQConfig config;
    private final Broker broker;
    private final MessageLog wal;
    private Server server;

    private MQServerImpl(MQConfig config, Broker broker, MessageLog wal) {
        this.config = config;
        this.broker = broker;
        this.wal = wal;
    }

    public static MQServerImpl create(MQConfig config) throws IOException {
        MessageLog wal = new FileWal(config.dataDirectory().resolve("mq.wal"));
        return create(config, new MonoBroker(config, wal), wal);
    }

    public static MQServerImpl create(MQConfig config, Broker broker) throws IOException {
        return create(config, broker, new FileWal(config.dataDirectory().resolve("mq.wal")));
    }

    public static MQServerImpl create(MQConfig config, Broker broker, MessageLog wal) {
        return new MQServerImpl(config, broker, wal);
    }

    @Override
    public void start() throws IOException {
        Recovery.recover(wal, broker);
        server = ServerBuilder.forPort(config.port())
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new MQServiceImpl(broker))
                .build()
                .start();
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    @Override
    public void shutdown() {
        if (server != null) {
            server.shutdown();
            try {
                server.awaitTermination();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        wal.close();
    }

    @Override
    public int port() {
        return server == null ? -1 : server.getPort();
    }

    @Override
    public void close() {
        shutdown();
    }

    public static void main(String[] args) throws Exception {
        MQServerImpl server = MQServerImpl.create(MQConfig.defaults());
        server.start();
        server.awaitTermination();
    }
}