package com.arth.sakimq.producer;

public interface Producer extends AutoCloseable {

    boolean createQueue(String queue);

    String publish(String queue, byte[] body);

    @Override
    void close();
}
