package com.arth.sakimq.broker.server;

import com.arth.sakimq.broker.config.MQConfig;
import com.arth.sakimq.broker.core.Broker;
import com.arth.sakimq.broker.core.MonoBroker;
import com.arth.sakimq.broker.storage.FileWal;
import com.arth.sakimq.broker.storage.MessageLog;
import com.arth.sakimq.broker.storage.Recovery;
import com.arth.sakimq.config.Config;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;


public final class MQServerImpl implements MQServer {

    private static final Logger log = LoggerFactory.getLogger(MQServerImpl.class);

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
        log.info("Creating SakiMQ server: port={}, dataDirectory={}, maxDeliveryCount={}",
                config.port(), config.dataDirectory(), config.maxDeliveryCount());
        MessageLog wal = new FileWal(config.dataDirectory());
        return create(config, new MonoBroker(config, wal), wal);
    }

    /**
     * 使用自定义 Broker 与 WAL 创建服务器。
     * <p>调用方必须保证 {@code broker} 已绑定到同一个 {@code wal}（例如通过 {@code new MonoBroker(config, wal)}），
     * 否则恢复只读 WAL、运行期新请求不落盘，重启后会丢失数据。</p>
     */
    public static MQServerImpl create(MQConfig config, Broker broker, MessageLog wal) {
        return new MQServerImpl(config, broker, wal);
    }

    @Override
    public void start() throws IOException {
        log.info("Starting SakiMQ server on port {} ...", config.port());
        Recovery.recover(wal, broker);
        server = ServerBuilder.forPort(config.port())
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .intercept(new ExceptionMappingInterceptor())
                .addService(new MQServiceImpl(broker))
                .build()
                .start();
        log.info("SakiMQ server started, listening on port {}", server.getPort());
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
            log.info("Shutting down SakiMQ server ...");
            server.shutdown();
            try {
                server.awaitTermination();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        wal.close();
        log.info("SakiMQ server stopped, WAL closed");
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
        MQServerImpl server = MQServerImpl.create(MQConfig.from(Config.load()));
        server.start();
        server.awaitTermination();
    }
}