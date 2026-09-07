package com.arth.sakimq.producer;

public interface Producer {

    String publish(String queue, byte[] body);
}
