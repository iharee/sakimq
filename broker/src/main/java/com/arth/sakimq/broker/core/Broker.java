package com.arth.sakimq.broker.core;

import java.time.Duration;
import java.util.Optional;
import com.arth.sakimq.model.Delivery;
import com.arth.sakimq.model.QueueStats;

public interface Broker {

    boolean createQueue(String queue);

    String publish(String queue, byte[] body);

    Optional<Delivery> consume(String queue, Duration visibilityTimeout, Duration waitTimeout);

    boolean ack(String queue, String receiptHandle);

    QueueStats stats(String queue);
}
