package com.arth.sakimq.broker.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import com.arth.sakimq.broker.config.MQConfig;
import com.arth.sakimq.broker.core.Broker;
import com.arth.sakimq.broker.core.MonoBroker;
import com.arth.sakimq.broker.storage.FileWal;
import com.arth.sakimq.broker.storage.MessageLog;
import com.arth.sakimq.broker.storage.Recovery;

import java.io.IOException;

public final class MQServer {

    private MQServer() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        MQConfig config = MQConfig.defaults();
        Broker broker = new MonoBroker(config);

        try (MessageLog wal = new FileWal(config.dataDirectory().resolve("mq.wal"))) {
            Recovery.recover(wal, broker);

            Server server = ServerBuilder.forPort(config.port())
                    .addService(new MQServiceImpl(broker))
                    .build()
                    .start();

            server.awaitTermination();
        }
    }
}
