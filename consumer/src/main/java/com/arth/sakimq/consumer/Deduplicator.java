package com.arth.sakimq.consumer;

import com.arth.sakimq.model.Delivery;

public interface Deduplicator {

    boolean isDuplicate(Delivery delivery);

    void markProcessed(Delivery delivery);
}
