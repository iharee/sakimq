package com.arth.sakimq.producer;

public class GrpcProducer implements Producer {

    @Override
    public String publish(String queue, byte[] body) {
        return "";
    }
}
